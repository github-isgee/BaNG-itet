package BaNG::Reporting;

use 5.010;
use strict;
use warnings;
use Encode qw(encode);
use BaNG::Config;
use BaNG::Converter;
use DBI;
use Date::Parse;
use IO::Socket;
use MIME::Lite;
use POSIX qw( strftime );
use Template;
use YAML::Tiny;

use Exporter 'import';
our @EXPORT = qw(
    $bangstat_dbh
    bangstat_db_connect
    bangstat_recentbackups
    bangstat_recentbackups_hours
    bangstat_recentbackups_job_details
    bangstat_recent_tasks
    bangstat_last_transfer
    bangstat_task_delete
    bangstat_task_jobs
    bangstat_jobs_by_jobstatus
    bangstat_report_pre_queue_error
    bangstat_report_queue_backupjob
    bangstat_report_start_backupjob
    bangstat_report_update_backupjob
    bangstat_report_finish_backupjob
    bangstat_set_taskmeta
    send_xymon_report
    mail_report
    xymon_report
    logit
    read_log
    read_global_log
    delete_logfiles
    error404
    print_formatted_table
);

our %serverconfig;
our $bangstat_dbh;

sub bangstat_db_connect {
    my ($ConfigBangstat) = @_;

    my $yaml       = YAML::Tiny->read($ConfigBangstat);
    my $DBdriver = $yaml->[0]{DBdriver};
    my $DBhostname = $yaml->[0]{DBhostname};
    my $DBusername = $yaml->[0]{DBusername};
    my $DBdatabase = $yaml->[0]{DBdatabase};
    my $DBpassword = $yaml->[0]{DBpassword};

    $bangstat_dbh = DBI->connect(
        "DBI:$DBdriver:database=$DBdatabase:host=$DBhostname:port=3306", $DBusername, $DBpassword,
        { PrintError => 0 }
    );

    return 0 unless $bangstat_dbh;
    return 1;
}

sub bangstat_recentbackups {
    my ( $host, $lastXdays ) = @_;

    $lastXdays ||= 14;
    my $BkpStartHour = 18;

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return () unless $conn;

    my $sth = $bangstat_dbh->prepare("
        SELECT
            TaskID, BkpGroup, TaskName, Description, Cron, BkpToHost, isThread,
            COUNT(JobID) as Jobs,
            MAX(JobStatus) as JobStatus,
            GROUP_CONCAT(DISTINCT ErrStatus order by ErrStatus) as ErrStatus,
            MIN(Start) as Start, MAX(Stop) as Stop,
            TIMESTAMPDIFF(Second, MIN(Start), MAX(Stop)) as Runtime,
            SUM(NumOfFiles) as NumOfFiles, SUM(TotFileSize) as TotFileSize,
            SUM(NumOfFilesCreated) as NumOfFilesCreated, SUM(NumOfFilesDel) as NumOfFilesDel,
            SUM(NumOfFilesTrans) as NumOfFilesTrans, SUM(TotFileSizeTrans) as TotFileSizeTrans
        FROM statistic
        LEFT JOIN statistic_task_meta USING (TaskID)
        WHERE Start > date_sub(concat(curdate(),' $BkpStartHour:00:00'), interval $lastXdays day)
            AND BkpFromHost like '$host'
        GROUP BY TaskID, BkpGroup, TaskName, Description, Cron, BkpToHost, isThread
        ORDER BY Start DESC;
    ");
    $sth->execute();

    my %RecentBackups;
    my %RecentBackupTimes;
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        my $BkpGroup    = $dbrow->{'BkpGroup'} || 'NA';
        my $Runtime     = $dbrow->{'Runtime'} ? $dbrow->{'Runtime'} / 60 : '-';
        push( @{$RecentBackups{"$host-$BkpGroup"}}, {
            TaskID       => $dbrow->{'TaskID'},
            JobID        => $dbrow->{'JobID'},
            Starttime    => $dbrow->{'Start'},
            Stoptime     => $dbrow->{'Stop'},
            Runtime      => time2human($Runtime),
            isThread     => $dbrow->{'isThread'},
            ErrStatus    => $dbrow->{'ErrStatus'},
            JobStatus    => $dbrow->{'JobStatus'},
            Jobs         => $dbrow->{'Jobs'},
            Cron         => $dbrow->{'Cron'},
            BkpGroup     => $BkpGroup,
            BkpHost      => $host,
            FilesCreated => num2human($dbrow->{'NumOfFilesCreated'}),
            FilesDel     => num2human($dbrow->{'NumOfFilesDel'}),
            FilesTrans   => num2human($dbrow->{'NumOfFilesTrans'}),
            SizeTrans    => num2human($dbrow->{'TotFileSizeTrans'},1024),
            TotFileSize  => num2human($dbrow->{'TotFileSize'},1024),
            NumOfFiles   => num2human($dbrow->{'NumOfFiles'}),
        });
        push( @{$RecentBackupTimes{"$host-$dbrow->{'BkpGroup'}"}}, {
            TaskID      => $dbrow->{'TaskID'},
            JobID       => $dbrow->{'JobID'},
            Starttime   => $dbrow->{'Start'},
            Host        => $host,
            BkpGroup    => $BkpGroup,
        });
    }
    $sth->finish();

    # depending on the current time, define when the next backup period starts
    my $now      = time;
    my $today    = `$serverconfig{path_date} -d \@$now +"%Y-%m-%d"`;
    my $one_day  = 24 * 3600;
    my $next_bkp = str2time("$today $BkpStartHour:00:00");
    $next_bkp += $one_day if ( $next_bkp < $now );

    # scan for missing backups
    foreach my $bkpgroup ( keys %RecentBackupTimes ) {
        my $nextBkpStart = $next_bkp;
        my $prevBkpStart = $nextBkpStart - $one_day;

        my @bkp                = @{$RecentBackupTimes{$bkpgroup}};
        my $missingBkpGroup    = $bkp[0]->{BkpGroup}    || 'NA';
        my $missingHost        = $bkp[0]->{Host}        || 'NA';

        foreach my $Xdays ( 1 .. $lastXdays ) {
            my $isMissing = 0;

            if ( !@bkp ) {
                # a backup is missing if list is already empty
                $isMissing = 1;
            } else {
                # or if no backup occured during that day
                my $latestbkp = str2time( $bkp[0]->{Starttime} );
                unless ( $latestbkp > $prevBkpStart
                      && $latestbkp < $nextBkpStart )
                {
                    $isMissing = 1;
                }
            }

            if ($isMissing) {
                # add empty entry for missing backups
                my $missingepoch = $prevBkpStart;
                my $missingday   = `$serverconfig{path_date} -d \@$missingepoch +"%Y-%m-%d"`;
                chomp $missingday;
                my $nobkp = {
                    Starttime   => $missingday,
                    Stoptime    => '',
                    Runtime     => '',
                    JobStatus   => '',
                    isThread    => '',
                    ErrStatus   => 99,
                    BkpGroup    => $missingBkpGroup,
                };
                splice( @{$RecentBackups{"$missingHost-$missingBkpGroup"}}, $Xdays - 1, 0, $nobkp );
            } else {
                # remove successful backups of that day from list
                while ( @bkp && str2time( $bkp[0]->{Starttime} ) > $prevBkpStart ) {
                    shift @bkp;
                }
            }

            # then look at previous day
            $nextBkpStart -= $one_day;
            $prevBkpStart -= $one_day;
        }
    }

    return %RecentBackups;
}

sub bangstat_recentbackups_hours {
    my ($lastXhours) = @_;
    $lastXhours ||= 24;

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my $sth = $bangstat_dbh->prepare("
    SELECT TaskID, JobID, BkpGroup, BkpFromHost, BkpToHost, isThread,
        MIN(Start) as Start, TIMESTAMPDIFF(Second, MIN(Start), MAX(Stop)) as Runtime,
        COUNT(JobID) as Jobs,
        SUM(NumOfFiles) as NumOfFiles, SUM(TotFileSize) as TotFileSize,
        SUM(NumOfFilesCreated) as NumOfFilesCreated, SUM(NumOfFilesDel) as NumOfFilesDel,
        SUM(NumOfFilesTrans) as NumOfFilesTrans, SUM(TotFileSizeTrans) as TotFileSizeTrans,
        GROUP_CONCAT(DISTINCT ErrStatus order by ErrStatus) as ErrStatus, MIN(JobStatus) as JobStatus
    FROM statistic
    WHERE Start > date_sub(NOW(), INTERVAL $lastXhours HOUR)
        AND BkpFromHost like '%'
    GROUP BY JobID, TaskID, BkpGroup, BkpFromHost, BkpToHost, isThread;
    ");
    $sth->execute();

    my %RecentBackupsAll;
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        my $Runtime     = $dbrow->{'Runtime'} ? $dbrow->{'Runtime'} / 60 : '-';
        push( @{$RecentBackupsAll{'Data'}}, {
            TaskID       => $dbrow->{'TaskID'},
            JobID        => $dbrow->{'JobID'},
            JobStatus    => $dbrow->{'JobStatus'},
            Jobs         => $dbrow->{'Jobs'},
            isThread     => $dbrow->{'isThread'},
            BkpHost      => $dbrow->{'BkpFromHost'},
            BkpGroup     => $dbrow->{'BkpGroup'} || 'NA',
            BkpToHost    => $dbrow->{'BkpToHost'},
            ErrStatus    => $dbrow->{'ErrStatus'},
            Starttime    => $dbrow->{'Start'},
            Runtime      => time2human($Runtime),
            FilesCreated => num2human($dbrow->{'NumOfFilesCreated'}),
            FilesDel     => num2human($dbrow->{'NumOfFilesDel'}),
            FilesTrans   => num2human($dbrow->{'NumOfFilesTrans'}),
            SizeTrans    => num2human($dbrow->{'TotFileSizeTrans'},1024),
            NumOfFiles   => num2human($dbrow->{'NumOfFiles'}),
            TotFileSize  => num2human($dbrow->{'TotFileSize'},1024),
        });
    }
    $sth->finish();

    return \%RecentBackupsAll;
}

sub bangstat_recentbackups_job_details {
    my ($jobid) = @_;

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my $sth = $bangstat_dbh->prepare("
        SELECT *,
            TIMESTAMPDIFF(Second, Start, Stop) as Runtime
        FROM statistic
        WHERE JobID = '$jobid'
        ORDER BY Start DESC;
    ");
    $sth->execute();

    my %RecentBackupsJobDetails;
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        my $Runtime     = $dbrow->{'Runtime'} ? $dbrow->{'Runtime'} / 60 : '-';
        my $BkpFromPath = $dbrow->{'BkpFromPath'};
        $BkpFromPath    =~ s/^:$/:\//g;
        push( @{$RecentBackupsJobDetails{'Data'}}, {
            TaskID       => $dbrow->{'TaskID'},
            JobID        => $dbrow->{'JobID'},
            Starttime    => $dbrow->{'Start'},
            Stoptime     => $dbrow->{'Stop'},
            Runtime      => time2human($Runtime),
            BkpFromPath  => $BkpFromPath ,
            BkpToPath    => $dbrow->{'BkpToPath'},
            isThread     => $dbrow->{'isThread'},
            ErrStatus    => $dbrow->{'ErrStatus'},
            JobStatus    => $dbrow->{'JobStatus'},
            Jobs         => 1,
            BkpGroup     => $dbrow->{'BkpGroup'} || 'NA',
            BkpHost      => $dbrow->{'BkpFromHost'},
            BkpToHost    => $dbrow->{'BkpToHost'},
            FilesCreated => num2human($dbrow->{'NumOfFilesCreated'}),
            FilesDel     => num2human($dbrow->{'NumOfFilesDel'}),
            FilesTrans   => num2human($dbrow->{'NumOfFilesTrans'}),
            SizeTrans    => num2human($dbrow->{'TotFileSizeTrans'},1024),
            TotFileSize  => num2human($dbrow->{'TotFileSize'},1024),
            NumOfFiles   => num2human($dbrow->{'NumOfFiles'}),
        });
    }
    $sth->finish();

    return \%RecentBackupsJobDetails;
}

sub bangstat_recent_tasks {

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my $sth = $bangstat_dbh->prepare("
        SELECT
            TaskID, TaskName, Description, Cron, BkpToHost, isThread,
            COUNT(JobID) as Jobs,
            MAX(JobStatus) as JobStatus,
            GROUP_CONCAT(DISTINCT ErrStatus order by ErrStatus) as ErrStatus,
            MIN(Start) as Start, MAX(Stop) as Stop,
            TIMESTAMPDIFF(Second, MIN(Start), MAX(Stop)) as Runtime,
            SUM(NumOfFiles) as NumOfFiles, SUM(TotFileSize) as TotFileSize,
            SUM(NumOfFilesCreated) as NumOfFilesCreated, SUM(NumOfFilesDel) as NumOfFilesDel,
            SUM(NumOfFilesTrans) as NumOfFilesTrans, SUM(TotFileSizeTrans) as TotFileSizeTrans
        FROM statistic
        LEFT JOIN statistic_task_meta USING (TaskID)
        WHERE Start > date_sub(NOW(), INTERVAL 24 HOUR)
        GROUP BY TaskID, TaskName, Description, Cron, BkpToHost, isThread
        ORDER BY TaskID DESC;
    ");
    $sth->execute();

    my %RecentTasks;
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        my $Runtime     = $dbrow->{'Runtime'} ? $dbrow->{'Runtime'} / 60 : '-';
        my $BkpFromPath = $dbrow->{'BkpFromPathRoot'};
        push( @{$RecentTasks{'Data'}}, {
            TaskID       => $dbrow->{'TaskID'},
            Taskname     => $dbrow->{'TaskName'},
            Description  => $dbrow->{'Description'},
            Cron         => $dbrow->{'Cron'},
            isThread     => $dbrow->{'isThread'},
            ErrStatus    => $dbrow->{'ErrStatus'},
            JobStatus    => $dbrow->{'JobStatus'},
            Jobs         => $dbrow->{'Jobs'},
            BkpToHost    => $dbrow->{'BkpToHost'},
            Starttime    => $dbrow->{'Start'},
            Runtime      => time2human($Runtime),
            FilesCreated => num2human($dbrow->{'NumOfFilesCreated'}),
            FilesDel     => num2human($dbrow->{'NumOfFilesDel'}),
            FilesTrans   => num2human($dbrow->{'NumOfFilesTrans'}),
            SizeTrans    => num2human($dbrow->{'TotFileSizeTrans'},1024),
            TotFileSize  => num2human($dbrow->{'TotFileSize'},1024),
            NumOfFiles   => num2human($dbrow->{'NumOfFiles'}),
        });
    }
    $sth->finish();

    return \%RecentTasks;
}

sub bangstat_last_transfer {

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my $sth = $bangstat_dbh->prepare("
        SELECT LastOne.*
        FROM (
            SELECT ID,TaskID,MAX(Start) as LastTransferDate,BkpFromHost,BkpGroup,BkpFromPath,NumOfFilesTrans,TotFileSizeTrans,NumOfFilesDel
            FROM statistic
            WHERE TotFileSizeTrans !='0'
            GROUP BY BkpFromPath
            ) AS LastOne
        INNER JOIN statistic ON statistic.ID = LastOne.ID
        ORDER BY LastTransferDate LIMIT 28;
    ");
    $sth->execute();

    my %LastTransfer;;
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        push( @{$LastTransfer{'Data'}}, {
            TaskID              => $dbrow->{'TaskID'},
            BkpFromHost         => $dbrow->{'BkpFromHost'},
            BkpGroup            => $dbrow->{'BkpGroup'},
            BkpFromPath         => $dbrow->{'BkpFromPath'},
            LastTransferDate    => $dbrow->{'LastTransferDate'},
            FilesDel            => num2human($dbrow->{'NumOfFilesDel'}),
            FilesTrans          => num2human($dbrow->{'NumOfFilesTrans'}),
            SizeTrans           => num2human($dbrow->{'TotFileSizeTrans'},1024)
        });
    }
    $sth->finish();

    return \%LastTransfer;
}

sub bangstat_task_delete {
    my ($taskid) = @_;

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my $sth = $bangstat_dbh->prepare("
        DELETE FROM statistic WHERE taskid LIKE '$taskid';
    ");
    $sth->execute();

    $sth->finish();

    return 1;
}

sub bangstat_task_jobs {
    my ($taskid) = @_;

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my $sth = $bangstat_dbh->prepare("
        SELECT
            TaskID, JobID, BkpFromHost, BkpGroup, BkpToHost, BkpFromPathRoot, isThread,
            MAX(JobStatus) as JobStatus,
            COUNT(JobID) as Jobs,
            GROUP_CONCAT(DISTINCT ErrStatus order by ErrStatus) as ErrStatus,
            MIN(Start) as Start, MAX(Stop) as Stop,
            TIMESTAMPDIFF(Second, MIN(Start), MAX(Stop)) as Runtime,
            SUM(NumOfFiles) as NumOfFiles, SUM(TotFileSize) as TotFileSize,
            SUM(NumOfFilesCreated) as NumOfFilesCreated, SUM(NumOfFilesDel) as NumOfFilesDel,
            SUM(NumOfFilesTrans) as NumOfFilesTrans, SUM(TotFileSizeTrans) as TotFileSizeTrans
        FROM statistic
        WHERE TaskID = '$taskid'
        GROUP BY JobID, TaskID, BkpToHost, BkpFromHost, BkpGroup, BkpFromPathRoot, isThread
        ORDER BY JobStatus, Start;

    ");
    $sth->execute();

    my %TaskJobs;
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        my $Runtime     = $dbrow->{'Runtime'} ? $dbrow->{'Runtime'} / 60 : '-';
        my $BkpFromPath = $dbrow->{'BkpFromPathRoot'};
        $BkpFromPath    =~ s/^:$/:\//g;
        push( @{$TaskJobs{'Data'}}, {
            TaskID       => $dbrow->{'TaskID'},
            JobID        => $dbrow->{'JobID'},
            Jobs         => $dbrow->{'Jobs'},
            Starttime    => $dbrow->{'Start'},
            Stoptime     => $dbrow->{'Stop'},
            Runtime      => time2human($Runtime),
            BkpFromPath  => $BkpFromPath ,
            BkpToPath    => $dbrow->{'BkpToPath'},
            isThread     => $dbrow->{'isThread'},
            ErrStatus    => $dbrow->{'ErrStatus'},
            JobStatus    => $dbrow->{'JobStatus'},
            BkpGroup     => $dbrow->{'BkpGroup'} || 'NA',
            BkpHost      => $dbrow->{'BkpFromHost'},
            BkpToHost    => $dbrow->{'BkpToHost'},
            FilesCreated => num2human($dbrow->{'NumOfFilesCreated'}),
            FilesDel     => num2human($dbrow->{'NumOfFilesDel'}),
            FilesTrans   => num2human($dbrow->{'NumOfFilesTrans'}),
            SizeTrans    => num2human($dbrow->{'TotFileSizeTrans'},1024),
            TotFileSize  => num2human($dbrow->{'TotFileSize'},1024),
            NumOfFiles   => num2human($dbrow->{'NumOfFiles'}),
        });
    }
    $sth->finish();

    return \%TaskJobs;
}

sub bangstat_jobs_by_jobstatus {
    my $taskid     = shift();
    my $host       = shift();
    my $group      = shift();
    my $jobid      = shift();
    my $jobstatus  = shift();
    my $lastXhours = shift();

    $lastXhours ||= 24;

    my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
    return '' unless $conn;

    my @where_parts  = ();
    my @where_params = ();

    if (defined($taskid)) {
        push(@where_parts, 'TaskID = ?');
        push(@where_params, $taskid);
    }

    if (defined($host)) {
        push(@where_parts, 'BkpFromHost = ?');
        push(@where_params, $host);
    }

    if (defined($group)) {
        push(@where_parts,  'BkpGroup = ?');
        push(@where_params, $group);
    }

    if (defined($jobid)) {
        push(@where_parts, 'JobID = ?');
        push(@where_params, $jobid);
    }

    if (defined($jobstatus)) {
        push(@where_parts, 'JobStatus = ?');
        push(@where_params, $jobstatus);
    }

    if (defined($lastXhours)) {
        # NB: we use "TimeStamp" instead of Start, because Start is NULL
        # when a job is in QUEUED status (Start is only set later, in
        # bangstat_report_start_backupjob() when the job really starts). 
        push(@where_parts, 'TimeStamp > date_sub(NOW(), INTERVAL ? HOUR)');
        push(@where_params, $lastXhours);
    }

    my $where_stmt = '';

    if (@where_parts) {
        $where_stmt = 'WHERE '.join(' AND ', @where_parts).' ';
    }

    my $sql =
        'SELECT '.
            'ID, '.
            'TaskID, '.
            'BkpFromHost, '.
            'BkpGroup, '.
            'JobID, '.
            'JobStatus, '.
            'ErrStatus, '.
            'Start, '.
            'Stop, '.
            'isThread, '.
            'NumOfFiles, '.
            'NumOfFilesTrans, '.
            'NumOfFilesCreated, '.
            'NumOfFilesDel, '.
            'TotFileSize, '.
            'TotFileSizeTrans, '.
            'BkpFromPath '.
        'FROM '.
            'statistic '.
        $where_stmt.
        'ORDER BY '.
            'ID';

    my $sth = $bangstat_dbh->prepare($sql);

    if (@where_params) {
        for (my $i = 0; $i < @where_params; $i++) {
            $sth->bind_param($i + 1, $where_params[$i]);
        }
    }

    $sth->execute();

    if ($sth->err()) {
        printf("SQL ERROR (%s): %s\n", $sth->err(), $sth->errstr());
    }

    my $backups_found = [];
    while ( my $dbrow = $sth->fetchrow_hashref() ) {
        my $Runtime = $dbrow->{'Runtime'} ? $dbrow->{'Runtime'} / 60 : '-';
        my $Start   = defined($dbrow->{'Start'}) ? $dbrow->{'Start'} : '-';
        my $Stop    = defined($dbrow->{'Stop'})  ? $dbrow->{'Stop'}  : '-';

        push(
            @$backups_found,
            {
                'TaskID'       => $dbrow->{'TaskID'},
                'BkpHost'      => $dbrow->{'BkpFromHost'},
                'BkpGroup'     => $dbrow->{'BkpGroup'} || '-',
                'JobID'        => $dbrow->{'JobID'},
                'JobStatus'    => $dbrow->{'JobStatus'},
                'ErrStatus'    => $dbrow->{'ErrStatus'},
                'Starttime'    => $Start,
                'Stoptime'     => $Stop,
                'Runtime'      => time2human($Runtime),
                'isThread'     => $dbrow->{'isThread'},
                'NumOfFiles'   => num2human($dbrow->{'NumOfFiles'}),
                'FilesTrans'   => num2human($dbrow->{'NumOfFilesTrans'}),
                'FilesCreated' => num2human($dbrow->{'NumOfFilesCreated'}),
                'FilesDel'     => num2human($dbrow->{'NumOfFilesDel'}),
                'TotFileSize'  => num2human($dbrow->{'TotFileSize'},1024),
                'SizeTrans'    => num2human($dbrow->{'TotFileSizeTrans'},1024),
                'BkpFromPath'  => $dbrow->{'BkpFromPath'}
            }
            );
    }

    $sth->finish();

    return $backups_found;
}

sub send_xymon_report {
    my ($report) = @_;

    return 1 unless $serverconfig{xymon_server};

    my @xymon_servers = split(' ', $serverconfig{xymon_server});

    foreach my $xymon_server (@xymon_servers) {
        my $socket = IO::Socket::INET->new(
            PeerAddr => $xymon_server,
            PeerPort => '1984',
            Proto    => 'tcp',
        );

        if ( defined $socket and $socket != 0 ) {
            $socket->print($report);
            $socket->close();
        }
    }
    return 1;
}

sub bangstat_report_pre_queue_error {
    my ( $taskid, $jobid, $host, $group, $startstamp, $endstamp, $path, $srcfolder, $targetpath, $errcode, $jobstatus, @outlines ) = @_;

    if ( $serverconfig{db_support} ) {
        $path =~ s/'//g;    # rm quotes to avoid errors in sql syntax
        my $isSubfolderThread = $hosts{"$host-$group"}->{hostconfig}->{BKP_THREAD_SUBFOLDERS} ? 1 : undef;

        # We don't set a start time, since the job is just queued
        # and not yet started.
        my $sql =
            'INSERT INTO statistic ('.
                'TaskID, '.
                'JobID, '.
                'Start, '.
                'Stop, '.
                'BkpFromHost, '.
                'BkpGroup, '.
                'BkpFromPath, '.
                'BkpFromPathRoot, '.
                'BkpToHost, '.
                'BkpToPath, '.
                'isThread, '.
                'ErrStatus, '.
                'JobStatus '.
                ') '.
            'VALUES '.
                '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

        logit( $taskid, $host, $group, "DB Report SQL command: $sql" ) if ( $serverconfig{verboselevel} >= 2 );

        my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
        if ( !$conn ) {
            logit( $taskid, $host, $group, "ERROR: Could not connect to DB to send bangstat report." );
            return 1;
        }

        my $sth = $bangstat_dbh->prepare($sql);

        $sth->bind_param( 1, $taskid);
        $sth->bind_param( 2, $jobid);
        $sth->bind_param( 3, $startstamp);
        $sth->bind_param( 4, $endstamp);
        $sth->bind_param( 5, $host);
        $sth->bind_param( 6, $group);
        $sth->bind_param( 7, $path);
        $sth->bind_param( 8, $srcfolder);
        $sth->bind_param( 9, $servername);
        $sth->bind_param(10, $targetpath);
        $sth->bind_param(11, $isSubfolderThread);
        $sth->bind_param(12, $errcode);
        $sth->bind_param(13, $jobstatus);

        $sth->execute() unless $serverconfig{dryrun};

        if ($sth->err()) {
            printf("SQL ERROR (%s): %s\n", $sth->err(), $sth->errstr());
        }

        $sth->finish();
        $bangstat_dbh->disconnect;

        logit( $taskid, $host, $group, "Bangstat queue_backup sent." );
    } else {
        logit( $taskid, $host, $group, "bangstat_queue_backup not sent - no DB-Support!" );
    }
    return 1;
}

sub bangstat_report_queue_backupjob {
    my ( $taskid, $jobid, $host, $group, $path, $srcfolder, $targetpath) = @_;

    if ( $serverconfig{db_support} ) {
        $path =~ s/'//g;    # rm quotes to avoid errors in sql syntax
        my $isSubfolderThread = $hosts{"$host-$group"}->{hostconfig}->{BKP_THREAD_SUBFOLDERS} ? 1 : undef;

        # We don't set a start time, since the job is just queued
        # and not yet started.
        my $sql =
            'INSERT INTO statistic ('.
                'TaskID, '.
                'JobID, '.
                'BkpFromHost, '.
                'BkpGroup, '.
                'BkpFromPath, '.
                'BkpFromPathRoot, '.
                'BkpToHost, '.
                'BkpToPath, '.
                'isThread, '.
                'ErrStatus, '.
                'JobStatus '.
                ') '.
            'VALUES '.
                '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

        logit( $taskid, $host, $group, "DB Report SQL command: $sql" ) if ( $serverconfig{verboselevel} >= 2 );

        my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
        if ( !$conn ) {
            logit( $taskid, $host, $group, "ERROR: Could not connect to DB to send bangstat report." );
            return 1;
        }

        my $sth = $bangstat_dbh->prepare($sql);

        $sth->bind_param( 1, $taskid);
        $sth->bind_param( 2, $jobid);
        $sth->bind_param( 3, $host);
        $sth->bind_param( 4, $group);
        $sth->bind_param( 5, $path);
        $sth->bind_param( 6, $srcfolder);
        $sth->bind_param( 7, $servername);
        $sth->bind_param( 8, $targetpath);
        $sth->bind_param( 9, $isSubfolderThread);
        $sth->bind_param(10, ERRSTATUS_NOERR);
        $sth->bind_param(11, JOBSTATUS_QUEUED);

        $sth->execute() unless $serverconfig{dryrun};

        if ($sth->err()) {
            printf("SQL ERROR (%s): %s\n", $sth->err(), $sth->errstr());
        }

        $sth->finish();
        $bangstat_dbh->disconnect;

        logit( $taskid, $host, $group, "Bangstat queue_backup sent." );
    } else {
        logit( $taskid, $host, $group, "bangstat_queue_backup not sent - no DB-Support!" );
    }
    return 1;
}

sub bangstat_report_start_backupjob {
    my ( $taskid, $jobid, $host, $group, $startstamp, $endstamp, $path, $srcfolder, $targetpath, $errcode, $jobstatus, @outlines ) = @_;

    if ( $serverconfig{db_support} ) {
        $path =~ s/'//g;    # rm quotes to avoid errors in sql syntax
        my $isSubfolderThread = $hosts{"$host-$group"}->{hostconfig}->{BKP_THREAD_SUBFOLDERS} ? 1 : undef;

        my $sql =
            'UPDATE '.
                'statistic '.
            'SET '.
                'ErrStatus = ?, '.
                'JobStatus = ?, '.
                'Start = FROM_UNIXTIME(?) '.
            'WHERE '.
                'TaskID = ? AND '.
                'JobID = ? AND '.
                'BkpFromHost = ? AND '.
                'BkpGroup = ? AND '.
                'BkpFromPath = ?';

        my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
        if ( !$conn ) {
            logit( $taskid, $host, $group, "ERROR: Could not connect to DB to send bangstat report." );
            return 1;
        }

        my $sth = $bangstat_dbh->prepare($sql);

        $sth->bind_param(1, $errcode);
        $sth->bind_param(2, $jobstatus);
        $sth->bind_param(3, $startstamp);
        $sth->bind_param(4, $taskid);
        $sth->bind_param(5, $jobid);
        $sth->bind_param(6, $host);
        $sth->bind_param(7, $group);
        $sth->bind_param(8, $path);

        $sth->execute() unless $serverconfig{dryrun};

        if ($sth->err()) {
            printf("SQL ERROR (%s): %s\n", $sth->err(), $sth->errstr());
        }

        $sth->finish();
        $bangstat_dbh->disconnect;

        logit( $taskid, $host, $group, "Bangstat start_backup sent." );
    } else {
        logit( $taskid, $host, $group, "bangstat_start_backup not sent - no DB-Support!" );
    }
    return 1;
}

sub bangstat_report_update_backupjob {
    my ( $taskid, $jobid, $host, $group, $endstamp, $path, $targetpath, $errcode, $jobstatus, @outlines ) = @_;

    if ( $serverconfig{db_support} ) {
        my %parse_log_keys = (
            'Number of files'                     => 'NumOfFiles',
            'Number of regular files transferred' => 'NumOfFilesTrans',
            'Number of created files'             => 'NumOfFilesCreated',
            'Number of deleted files'             => 'NumOfFilesDel',
            'Number of files transferred'         => 'NumOfFilesTrans',
            'Total file size'                     => 'TotFileSize',
            'Total transferred file size'         => 'TotFileSizeTrans',
            'Literal data'                        => 'LitData',
            'Matched data'                        => 'MatchData',
            'File list size'                      => 'FileListSize',
            'File list generation time'           => 'FileListGenTime',
            'File list transfer time'             => 'FileListTransTime',
            'Total bytes sent'                    => 'TotBytesSent',
            'Total bytes received'                => 'TotBytesRcv',
        );

        my %log_values;
        foreach my $logkey ( keys %parse_log_keys ) {
            $log_values{$parse_log_keys{$logkey}} = "NULL";
        }

        foreach my $outline (@outlines) {
            next unless $outline =~ m/:/;
            chomp $outline;
            my ( $key, $value ) = split( ': ', $outline );
            foreach my $logkey ( keys %parse_log_keys ) {
                if ( $logkey eq $key ) {
                    $value =~ s/^\D*([\d\.,]+).*?$/$1/;
                    $value =~ s/,//g;
                    $log_values{$parse_log_keys{$logkey}} = $value;
                }
            }
        }

        $path =~ s/'//g;    # rm quotes to avoid errors in sql syntax

        my $sql =
            'UPDATE '.
                'statistic '.
            'SET '.
                'ErrStatus = ?, '.
                'JobStatus = ?, '.
                'Stop = FROM_UNIXTIME(?), '.
                'NumOfFiles = ?, '.
                'NumOfFilesTrans = ?, '.
                'NumOfFilesCreated = ?, '.
                'NumOfFilesDel = ?, '.
                'TotFileSize = ?, '.
                'TotFileSizeTrans = ?, '.
                'LitData = ?, '.
                'MatchData = ?, '.
                'FileListSize = ?, '.
                'FileListGenTime = ?, '.
                'FileListTransTime = ?, '.
                'TotBytesSent = ?, '.
                'TotBytesRcv = ? '.
            'WHERE '.
                'TaskID = ? AND '.
                'JobID = ? AND '.
                'BkpFromHost = ? AND '.
                'BkpGroup = ? AND '.
                'BkpFromPath = ?';

        my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
        if ( !$conn ) {
            logit( $taskid, $host, $group, 'ERROR: Could not connect to DB to send bangstat report.' );
            return 1;
        }

        my $sth = $bangstat_dbh->prepare($sql);

        $sth->bind_param( 1, $errcode);
        $sth->bind_param( 2, $jobstatus);
        $sth->bind_param( 3, $endstamp);
        $sth->bind_param( 4, $log_values{NumOfFiles});
        $sth->bind_param( 5, $log_values{NumOfFilesTrans});
        $sth->bind_param( 6, $log_values{NumOfFilesCreated});
        $sth->bind_param( 7, $log_values{NumOfFilesDel});
        $sth->bind_param( 8, $log_values{TotFileSize});
        $sth->bind_param( 9, $log_values{TotFileSizeTrans});
        $sth->bind_param(10, $log_values{LitData});
        $sth->bind_param(11, $log_values{MatchData});
        $sth->bind_param(12, $log_values{FileListSize});
        $sth->bind_param(13, $log_values{FileListGenTime});
        $sth->bind_param(14, $log_values{FileListTransTime});
        $sth->bind_param(15, $log_values{TotBytesSent});
        $sth->bind_param(16, $log_values{TotBytesRcv});
        $sth->bind_param(17, $taskid);
        $sth->bind_param(18, $jobid);
        $sth->bind_param(19, $host);
        $sth->bind_param(20, $group);
        $sth->bind_param(21, $path);

        $sth->execute() unless $serverconfig{dryrun};

        if ($sth->err()) {
            printf("SQL ERROR (%s): %s\n", $sth->err(), $sth->errstr());
        }

        $sth->finish();
        $bangstat_dbh->disconnect;

        logit( $taskid, $host, $group, "Set jobstatus to $jobstatus for host $host group $group jobid $jobid" );
    } else {
        logit( $taskid, $host, $group, "bangstat_update_backupjob not sent - no DB-Support!" );
    }

    return 1;
}

sub bangstat_report_finish_backupjob {
    my ( $taskid, $jobid, $host, $group, $jobstatus ) = @_;

    if ( $serverconfig{db_support} ) {
        my $sql =
            'UPDATE '.
                'statistic '.
            'SET '.
                'JobStatus = ? '.
            'WHERE '.
                'BkpFromHost = ? AND '.
                'BkpGroup = ? AND '.
                'JobID = ?';

        my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
        if ( !$conn ) {
            logit( $taskid, $host, $group, "ERROR: Could not connect to DB to set jobstatus to $jobstatus for host $host group $group" );
            return 1;
        }

        my $sth = $bangstat_dbh->prepare($sql);

        $sth->bind_param(1, $jobstatus);
        $sth->bind_param(2, $host);
        $sth->bind_param(3, $group);
        $sth->bind_param(4, $jobid);

        $sth->execute() unless $serverconfig{dryrun};

        if ($sth->err()) {
            printf("SQL ERROR (%s): %s\n", $sth->err(), $sth->errstr());
        }

        $sth->finish();

        logit( $taskid, $host, $group, "Set jobstatus to $jobstatus for host $host group $group jobid $jobid" );
    } else {
        logit( $taskid, $host, $group, "bangstat_finish_backupjob not sent - no DB-Support!" );
    }

    return 1;
}

sub bangstat_set_taskmeta {
    my ( $taskid, $host, $group, $cron, $override ) = @_;

    if ( $serverconfig{db_support} ) {
        $host  ||= 'BULK';
        $group ||= '*';

        my $taskName    = $host ."_". $group;
        my $description = $override || "" ;

        $description = get_taskmeta($host, $group) unless $override;

        print "TaskID: $taskid Taskname: $taskName Description: $description Cron: $cron\n" if $serverconfig{verbose};

        my $sql = qq(
            INSERT INTO statistic_task_meta (
            TaskID, TaskName, Description, Cron
            ) VALUES (
            '$taskid', '$taskName', '$description', '$cron')
        );

        logit( $taskid, $host, $group, "DB Report SQL command: $sql" ) if ( $serverconfig{verboselevel} >= 2 );

        my $conn = bangstat_db_connect( $serverconfig{config_bangstat} );
        if ( !$conn ) {
            logit( $taskid, $host, $group, "ERROR: Could not connect to DB to send bangstat report." );
            return 1;
        }

        my $sth = $bangstat_dbh->prepare($sql);
        $sth->execute() unless $serverconfig{dryrun};
        $sth->finish();
        $bangstat_dbh->disconnect;

        logit( $taskid, $host, $group, "Bangstat task_meta sent." );
    } else {
        logit( $taskid, $host, $group, "bangstat_set_taskmeta not sent - no DB-Support!" );
    }

    return 1;
}

sub mail_report {
    my ( $taskid, $host, $group, %RecentBackups ) = @_;

    my $status = $hosts{"$host-$group"}->{errormsg} ? 'warnings' : 'success';

    unless ( $status eq 'success' ) {
        my $RecentBackups = {
            RecentBackups => \%RecentBackups,
            Hosts         => \%hosts,
            Group         => "$host-$group",
            Errormsg      => $hosts{"$host-$group"}->{errormsg},
        };

        my $tt = Template->new(
            START_TAG    => '<%',
            END_TAG      => '%>',
            INCLUDE_PATH => "$prefix/views",
        );

        my $mail_msg = MIME::Lite->new(
            From    => 'root@phys.ethz.ch',
            To      => $serverconfig{report_to},
            Type    => 'multipart/alternative',
            Subject => "Backup report of ($host-$group): $status",
        );

        foreach my $mailtype (qw(plain html)) {
            my $report;
            $tt->process( "report-mail_$mailtype.tt", $RecentBackups, \$report )
                or logit( $taskid, $host, $group, 'ERROR generating mail report template: ' . $tt->error() );

            my $mail_att = MIME::Lite->new(
                Type     => 'text',
                Data     => $report,
                Encoding => 'quoted-printable',
            );
            $mail_att->attr( 'content-type' => "text/$mailtype; charset=UTF-8" );
            $mail_msg->attach($mail_att);
        }

        unless ( $serverconfig{dryrun} ) {
            $mail_msg->send or logit( $taskid, $host, $group, 'mail_report error' );
        }

        logit( $taskid, $host, $group, 'Mail report sent.' );
    }
    return 1;
}

sub xymon_report {
    my ( $taskid, $host, $group, %RecentBackups ) = @_;

    my $topcolor = 'green';
    my $tc_helper = 0;
    my $errcode;
    foreach my $key ( sort keys %RecentBackups ) {
        if ( $RecentBackups{$key}[0]{JobStatus} eq "-1" ) {
            $topcolor = 'clear';
        } elsif ( $RecentBackups{$key}[0]{JobStatus} eq "-2" ) {
            $tc_helper = 1;
        }else{
            $errcode = $RecentBackups{$key}[0]{ErrStatus};
            my @errorcodes = split( ',', $errcode );
            foreach my $code (@errorcodes) {
                next if $code eq '0';     # no errors
                next if $code eq '24';    # vanished source files
                next if $code eq '99';    # no last_bkp
                if ( $code eq '23' || $code eq '12' ) {
                    $topcolor = 'yellow'; # partial transfer
                    next;
                }
                $topcolor = 'red';
            }
        }
    }
    $topcolor = 'yellow' unless %RecentBackups;
    $topcolor = 'red' if $tc_helper;

    my $RecentBackups = {
        RecentBackups => \%RecentBackups,
        Hosts         => \%hosts,
        Group         => "$host-$group",
        xymonTopColor => $topcolor,
        Errormsg      => $hosts{"$host-$group"}->{errormsg},
    };

    my $STATUSTTL = 2160;     # (2160=>1.5d) Time in min until page becomes purple
    my $DATE      = `$serverconfig{path_date}`;
    chomp $DATE;

    my $xymonreport = "status+$STATUSTTL $host.bkp $topcolor $DATE (TTL=$STATUSTTL min)\n";

    my $tt = Template->new(
        START_TAG    => '<%',
        END_TAG      => '%>',
        INCLUDE_PATH => "$prefix/views",
    );
    my $report;
    $tt->process( 'report-xymon.tt', $RecentBackups, \$report )
        or logit( $taskid, $host, $group, "ERROR generating xymon report template: " . $tt->error() );
    $xymonreport .= $report;

    send_xymon_report($xymonreport) unless $serverconfig{dryrun};
    print "Xymon Report: $xymonreport\n" if $serverconfig{dryrun};
    logit( $taskid, $host, $group, "xymon report sent." );

    return 1;
}

sub logit {
    my ( $taskid, $host, $group, $msg ) = @_;
    $host  ||= '*';
    $group ||= '*';
    my $timestamp     = strftime '%b %d %H:%M:%S', localtime;
    my $logmonth      = strftime '%Y-%m',          localtime;
    my $logdate       = strftime $serverconfig{global_log_date}, localtime;
    my $logfolder     = "$serverconfig{path_logs}/${host}_${group}";
    my $globallogfile = "$serverconfig{path_logs}/global_$logmonth.log";
    my $logfile       = "$logfolder/$logdate.log";
    my $logmessage    = "$timestamp $host-$group($taskid) : $msg";
    $logmessage .= "\n" unless ( $logmessage =~ m/\n$/ );

    # write selection of messages to global logfile
    my $selection = qr{
        Queueing \s backup \s for |
        Skipping \s because |
        reorder \s queue |
        sleep |
        NOCACHE \s selected |
        working \s on |
        PID |
        finished \s with |
        Backup \s successful |
        ERROR |
        Wipe \s host |
        Wipe \s existing |
        Wipe \s successful |
        Wipe \s WARNING |
        Delete \s logfile |
        Delete \s btrfs \s subvolume
    }x;

    if ( $serverconfig{verbose} ) {
        if ( $serverconfig{dryrun} ) {
            unless ( $group eq 'GLOBAL' || $host eq 'SERVER' ) {

                # write into daily logfile per host_group
                print "Write to HOST log: $logmessage";
            }
            if ( $logmessage =~ /$selection/ || $group eq 'GLOBAL' || $host eq 'SERVER' ) {
                print "Write to GLOBAL log: $logmessage";
            }
        } else {
            print encode('utf-8', $logmessage);
        }
    }

    unless ( $serverconfig{dryrun} ) {
        unless ( $group eq 'GLOBAL' || $host eq 'SERVER' ) {

            # write into daily logfile per host_group
            mkdir($logfolder) unless -d $logfolder;
            open my $log, '>>', $logfile or print "ERROR opening logfile $logfile: $!\n";
            print {$log} encode('utf-8', $logmessage);
            close $log;
        }

        if ( $logmessage =~ /$selection/ || $group eq 'GLOBAL' || $host eq "SERVER" ) {
            open my $log, '>>', $globallogfile or print "ERROR opening logfile $globallogfile: $!\n";
            print {$log} encode('utf-8', $logmessage);
            close $log;
        }
    }

    if ( $logmessage =~ /warn|error/i ) {
        $hosts{"$host-$group"}{errormsg} .= $logmessage;
    }

    return 1;
}

sub read_log {
    my ( $host, $group, $show_logs_number ) = @_;

    my %parsed_logdata;
    my $logfolder = "$serverconfig{path_logs}/${host}_${group}";
    my @logfiles  = glob("$logfolder/*.log");
    $show_logs_number = $#logfiles + 1 if ( $#logfiles < $show_logs_number );

    foreach my $logfile ( @logfiles[ -$show_logs_number .. -1 ] ) {
        open LOGDATA, '<', $logfile or print "ERROR opening logfile $logfile: $!\n";
        my @logdata = <LOGDATA>;
        close LOGDATA;

        foreach my $logline (@logdata) {
            if ( $logline =~ qr{
                    (?<logdate> \w{3}\s\d{2} ) \s
                    (?<logtime> \d{2}:\d{2}:\d{2} ) \s
                    (?<hostgroup> [^:]* )\s:\s
                    (?<message> .* )
                }x )
            {
                push( @{ $parsed_logdata{$+{logdate}} }, {
                    date      => $+{logdate},
                    time      => $+{logtime},
                    hostgroup => $+{hostgroup},
                    message   => $+{message},
                });
            } else {
                $parsed_logdata{( sort keys %parsed_logdata )[-1]}[-1]->{message} .= "<br />$logline";
            }
        }
    }

    return \%parsed_logdata;
}

sub read_global_log {

    my %parsed_logdata;
    my $logmonth = strftime '%Y-%m', localtime;
    my $globallogfile = "$serverconfig{path_logs}/global_$logmonth.log";

    open LOGDATA, '<', $globallogfile or print "ERROR opening logfile $globallogfile: $!\n";
    my @logdata = <LOGDATA>;
    close LOGDATA;

    foreach my $logline (@logdata) {
        if ( $logline =~ qr{
                (?<logdate> \w{3}\s\d{2} ) \s
                (?<logtime> \d{2}:\d{2}:\d{2} ) \s
                (?<hostgroup> [\w-]* )\(
                (?<taskid> \d* ) \) \s:\s
                (?<message> .* )
            }x )
        {
            my $msg       = $+{message};
            my $logdate   = $+{logdate};
            my $logtime   = $+{logtime};
            my $hostgroup = $+{hostgroup};
            my $taskid    = $+{taskid};

            if ( $msg =~ /ERR/ ) {
                push( @{ $parsed_logdata{$logdate} }, {
                    date      => $logdate,
                    time      => $logtime,
                    hostgroup => $hostgroup,
                    taskid    => $taskid,
                    message   => $msg,
                });
            }
        }
    }

    return \%parsed_logdata;
}

sub delete_logfiles {
    my ( $host, $group, $taskid, @wipedirs ) = @_;
    $taskid ||= 0;

    foreach my $dir (@wipedirs) {
        my ($logdate) = $dir =~ /.*\/(\d{4}\.\d{2}\.\d{2})/;
        $logdate =~ s/\./\-/g;

        my $logfolder = "$serverconfig{path_logs}/${host}_${group}";
        my $logfile   = "$logfolder/$logdate.log";

        my ($dirtest) = $dir =~ /(.*\/\d{4}\.\d{2}\.\d{2})/;
        $dirtest .= "_*";
        my @bkpfolderexists = glob($dirtest);

        if (scalar @bkpfolderexists == 0) {
            my $rmcmd = 'rm -f';
            $rmcmd    = "echo $rmcmd" if $serverconfig{dryrun};

            logit( $taskid, $host, $group, "Delete logfile $logfile" );
            system("$rmcmd $logfile") and logit( $taskid, $host, $group, "ERROR: deleting logfile $logfile: $!" );
        } else {
            logit( $taskid, $host, $group, "Delete logfile $logfile aborted, $dirtest still exist!" );
        }
    }

    return 1;
}

sub error404 {
    my ($title) = @_;
    $title ||= 'An error occured.';

    Dancer::Continuation::Route::ErrorSent->new(
        return_value => Dancer::Error->new(
            code  => 404,
            title => $title,
        )->render()
    )->throw;
}

##############################################################################
#                                                                            #
# print_formatted_table($data, $formats)                                     #
#                                                                            #
# prints a dynamically formatted, data-aligned table to                      #
# STDOUT. the sub will compute the required minimal and                      #
# maximal column widths from table data ($data) and for-                     #
# matting constraints ($formats). the overall length of                      #
# the table isn't limited, so displaying a larger table                      #
# requires a fairly large terminal.                                          #
#                                                                            #
# params:                                                                    #
#                                                                            #
#   $data:                                                                   #
#          the table body data, in the following format:                     #
#          a reference to an array of hashreferences.                        #
#          each array item (hashref) represents a row.                       #
#          each hashref points to a hash containing                          #
#          key-value pairs for each column on that row.                      #
#                                                                            #
#          example:                                                          #
#                                                                            #
#          my $data = [                                                      #
#              { 'colA' => '12345', 'colB' => 'Task one'   },                #
#              { 'colA' => '67890', 'colB' => 'Task two'   },                #
#              { 'colA' => '13579', 'colB' => 'Task three' }                 #
#              ];                                                            #
#                                                                            #
#   $formats:                                                                #
#          table metadata, like custom table column titles,                  #
#          and column formatting information. this is an array               #
#          of hash references. it *must* contain the following               #
#          key-value pairs:                                                  #
#          - colkey: a column name that matches a column key                 #
#                    in $data. only columns listed in $formats               #
#                    will actually be printed to STDOUT. note                #
#                    that colkey must exist for each row in $data.           #
#                    it is however allowed to omit displaying                #
#                    some columns existing in $data by just not              #
#                    mentioning them in $formats. also note that             #
#                    the order of the array referenced by                    #
#                    $formats actually defines the column output             #
#                    order (and not the natural column order in              #
#                    $data!).                                                #
#          - title:  a custom column title (text). should not                #
#                    exceed "maxlen" characters (see below).                 #
#          - align:  'l' for left text alignment in the output               #
#                    of the referenced row's body data. 'r' for              #
#                    right text alignment.                                   #
#          - maxlen: maximum length (width) of the referenced                #
#                    column. table cell strings that are longer              #
#                    than maxlen characters will be truncated to             #
#                    maxlen characters. note that the "title"                #
#                    attribute should have a string length that              #
#                    is lower or equal to maxlen.                            #
#          example:                                                          #
#                                                                            #
#          my $formats = [                                                   #
#              {                                                             #
#                'colkey' => 'colA',                                         #
#                'title'  => 'Task ID',                                      #
#                'align'  => 'r',                                            #
#                'maxlen' =>  8                                              #
#              },                                                            #
#              {                                                             #
#                'colkey' => 'colB',                                         #
#                'title'  => 'Desc',                                         #
#                'align'  => 'l',                                            #
#                'maxlen' => 12                                              #
#              }                                                             #
#              ];                                                            #
#                                                                            #
##############################################################################
sub print_formatted_table {
    my ($data, $formats) = @_;

    # compute minimal column lengths for each
    # column, respecting maxlen.
    my $col_minlengths = {};

    for (my $i = 0; $i < @$formats; $i++) {
        my $colkey    = $formats->[$i]{'colkey'};
        my $colmaxlen = $formats->[$i]{'maxlen'};
        my $coltitle  = $formats->[$i]{'title'};

        my $colminlen = length($coltitle);

        foreach my $row (@$data) {
            my $cellstr = defined($row->{$colkey})
                ? $row->{$colkey}
                : '';

            my $cellstrlen = length($cellstr);

            # cap the cell value length at colmaxlen
            if ($cellstrlen > $colmaxlen) {
                $cellstrlen = $colmaxlen;
            }

            # update minimum column length
            # if longer cell value found
            if ($cellstrlen > $colminlen) {
                $colminlen = $cellstrlen;
            }
        }

        # minimum column length should never
        # exceed maxlen
        if ($colminlen > $colmaxlen) {
            $colminlen = $colmaxlen;
        }

        $col_minlengths->{$colkey} = $colminlen;
    }

    # create printf format strings for each column
    my $printf_formats = {};

    for (my $i = 0; $i < @$formats; $i++) {
        my $colkey = $formats->[$i]{'colkey'};
        my $align  = $formats->[$i]{'align'};

        my $colminlength = $col_minlengths->{$colkey};

        if ($align eq 'r') {
            $printf_formats->{$colkey} = sprintf(
                '%%%ss',
                $colminlength
                );

        } elsif ($align eq 'l') {
            $printf_formats->{$colkey} = sprintf(
                '%%-%ss',
                $colminlength
                );

        } else {
            printf(
                STDERR
                "invalid alignment specifier: %s for column %s.\n",
                $align,
                $colkey
                );

            # fall back to left alignment
            $printf_formats->{$colkey} = "%${colminlength}s";
        }
    }

    # output table column header
    for (my $i = 0; $i < @$formats; $i++) {
        my $colkey       = $formats->[$i]{'colkey'};
        my $coltitle     = $formats->[$i]{'title'};
        my $colminlength = $col_minlengths->{$colkey};

        # truncate column title to colminlength, if longer.
        # note: colminlength might be tttt

        if (length($coltitle) > $colminlength) {
            $coltitle = substr($coltitle, 0, $colminlength);
        }

        my $varspace = $i < @$formats - 1 ? ' ' : '';
        printf($printf_formats->{$colkey}.$varspace, $coltitle);
    }

    printf("\n");

    # output table column header horizontal rules
    for (my $i = 0; $i < @$formats; $i++) {
        my $colkey       = $formats->[$i]{'colkey'};
        my $colminlength = $col_minlengths->{$colkey};

        my $varspace = $i < @$formats - 1 ? ' ' : '';
        printf(('-' x $colminlength).$varspace);
    }

    printf("\n");

    # output table body
    for (my $i = 0; $i < @$data; $i++) {
        for (my $j = 0; $j < @$formats; $j++) {
            my $colkey = $formats->[$j]{'colkey'};

            my $value = exists($data->[$i]{$colkey})
                ? $data->[$i]{$colkey}
                : '';

            my $colminlength = $col_minlengths->{$colkey};

            # truncate if needed
            if (length($value) > $colminlength) {
                $value = substr($value, 0, $colminlength);
            }

            my $varspace = $j < @$formats - 1 ? ' ' : '';
            printf($printf_formats->{$colkey}.$varspace, $value);
        }

        printf("\n");
    }
}

1;
