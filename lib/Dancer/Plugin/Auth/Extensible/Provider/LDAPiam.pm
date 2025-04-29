package Dancer::Plugin::Auth::Extensible::Provider::LDAPiam;

use 5.010;
use strict;
use warnings;
use Net::LDAP;

my $config = Dancer::Config::setting('plugins')->{'Auth::Extensible'}->{'realms'}->{'ldap'};

sub new {
    my ($class, $realm_settings) = @_;

    my $self = {
        realm_settings => $realm_settings,
    };

    return bless $self => $class;
}

sub realm_settings {
    shift->{realm_settings} || {};
}

sub authenticate_user {
    my ($self, $username, $password) = @_;

    my $ldap = Net::LDAP->new($config->{server}, scheme => 'ldaps');
    my $bind = $ldap->bind(
        "cn=$username,".$config->{user_base_dn},
        'password' => $password
        );

    return 0 unless $bind;

    $ldap->unbind;
    $ldap->disconnect;

    my $authenticated = 0;
    $authenticated = 1 if( $bind->code == 0 && not $bind->is_error );

    return $authenticated;
}

sub get_user_details {
    my ($self, $username) = @_;

    my $ldap = Net::LDAP->new($config->{server}, scheme => 'ldaps');
    my $bind = $ldap->bind();

    my $ldap_result = $ldap->search(
       base   => $config->{user_base_dn},
       filter => "(cn=$username)",
       attrs  => ['cn'],
    );

    $ldap->unbind;
    $ldap->disconnect;

    return {} unless $ldap_result->count == 1;

    my $user_object  = ($ldap_result->entries)[0];

    my $user_details = {
        dn => $user_object->dn(),
        cn => ($user_object->get_value('cn'))[0],
        };

    return $user_details;
}

sub get_user_roles {
    my ($self, $username) = @_;

    if (!open(F, '<', $config->{proxy_user_pwfile})) {
        printf(
            STDERR
            "ERROR: cannot open proxy_user_pwfile (%s)\n",
            $config->{proxy_user_pwfile}
            );

        return [];
    }

    my $proxy_user_pw = <F>;
    chomp($proxy_user_pw);
    close(F);

    my $ldap = Net::LDAP->new($config->{server}, scheme => 'ldaps');
    my $bind = $ldap->bind($config->{proxy_user_dn}, password => $proxy_user_pw);

    my $ldap_result = $ldap->search(
        base   => $config->{group_base_dn},
        filter => $config->{group_filter},
        attrs  => ['cn', 'memberUid'],
        );

    $ldap->unbind;
    $ldap->disconnect;

    my $user_groups = [];
    foreach my $group ($ldap_result->entries) {
        my $has_members   = $group->get_value('memberUid');
        my @group_members = @{ $group->get_value( 'memberUid', asref => 1 ) } if $has_members;
        if ( grep { $_ eq $username } @group_members ) {
            my $groupname = ($group->get_value('cn'))[0];
            push( @$user_groups, $groupname );
        }
    }

    return [ sort(@$user_groups) ];
}

1;
