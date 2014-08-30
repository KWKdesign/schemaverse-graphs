#! /usr/bin/perl

use 5.010;
use strict;
use warnings;

use File::Pid;
my $pidfile = File::Pid->new({file => 'update.pl.pid' });
exit if $pidfile->running();
$pidfile->write();

use Data::Dump;
use List::Util qw/max min/;
use DBI;
use POSIX qw/strftime/;
use JSON qw/to_json from_json/;
use Net::Amazon::S3;

my $config = eval{ do 'config.pl' } or die 'No Config';

my $dbh = DBI->connect(
    'dbi:Pg:host='.$config->{host}.';database=schemaverse',
    $config->{user},
    $config->{pass},
    {
        PrintWarn   => 0,
        PrintError  => 0,
        RaiseError  => 1,
        AutoCommit  => 1,
    }
) or die "$!";

sub check_lock {
    my $locked = $dbh->selectrow_array(q/
        select status = 'Locked' from status;
    /);
    die strftime('%Y-%m-%d %H:%M:%S', localtime) . ' - Locked' if $locked;
}
check_lock();

my $round = $dbh->selectrow_array(q/
    select last_value from round_seq;
/);

say strftime('%Y-%m-%d %H:%M:%S', localtime), ' - Start ' . $round;

my $s3 = Net::Amazon::S3->new({
    aws_access_key_id		=> $config->{aws_access_key_id},
    aws_secret_access_key	=> $config->{aws_secret_access_key},
});
my $c = Net::Amazon::S3::Client->new( s3 => $s3 );
my $bucket = $c->bucket( name => $config->{bucket_name} );

my $colors = [qw/1f77b4 aec7e8 ff7f0e ffbb78 2ca02c 98df8a d62728 ff9896 9467bd c5b0d5 8c564b c49c94 e377c2 f7b6d2 7f7f7f c7c7c7 bcbd22 dbdb8d 17becf 9edae5/];
my $luma_threshold = 5;
sub check_luma {
    my $rgb = shift;
	# return 0 unless defined $rgb;
    return 0 unless $rgb =~ /[[:xdigit:]]{6}/;
    my ( $r,$g,$b ) = $rgb =~ m/[[:xdigit:]]{2}/g;
    return $luma_threshold <
        ( 0.2126 * hex $r ) + ( 0.7152 * hex $g ) + ( 0.0722 * hex $b ); # luma objective
}

my( $players, $stats, $name );

$name = 'Planets Conquered';
$stats = $dbh->selectall_hashref(q/
    select conqueror_id::text p, count(1) v,
    ( select last_value from tic_seq ) t,
    ( select last_value from round_seq ) r
    from planets
    where conqueror_id in (
        select id from player_list
        where 1=1
        and rgb is not null
        and symbol is not null
    )
    group by conqueror_id
    ;
/, 'p');
update_obj('planets');

$name = 'Fuel Mined';
$stats = get_stats('fuel_mined');
update_obj('fuel_mined');

$name = 'Damage Done';
$stats = get_stats('damage_done');
update_obj('damage_done');

$name = 'Damage Taken';
$stats = get_stats('damage_taken');
update_obj('damage_taken');

$name = 'Distance Travelled';
$stats = get_stats('distance_travelled');
update_obj('distance');

$name = 'Ship Upgrades';
$stats = get_stats('ship_upgrades');
update_obj('upgrades');

$name = 'Ships Built';
$stats = get_stats('ships_built');
update_obj('ships_built');

$name = 'Ships Lost';
$stats = get_stats('ships_lost');
update_obj('ships_lost');

sub get_stats {
    check_lock();
    my $field = shift;
    my $stats = $dbh->selectall_hashref(q/
        select player_id::text p, /. $field . q/ v,
        ( select last_value from tic_seq ) t,
        ( select last_value from round_seq ) r
        from current_player_stats
        where /. $field . q/ > 0
        and player_id in (
            select id from player_list
            where 1=1
            and rgb is not null
            and symbol is not null
        )
        ;     
    /, 'p');    
    return $stats;
}

sub get_player_info {
    my( $ids ) = @_;
	# $ids = [ grep { $_ ne '' } @$ids ];
    my $p = $dbh->selectall_hashref(q/
        select id p, username n, symbol s, rgb c
        from player_list where id in ( /
        . join(',',@$ids)
        . q/ );
    /, 'p');
    delete $p->{$_}->{p} for keys %$p;
    for ( keys %$p ) {
        next if check_luma( $p->{$_}->{c} );
        $p->{$_}->{c} = $colors->[ $_ % scalar @$colors ]
    }
    return $p;
}

sub update_obj {
    my $stat = shift;
    my ( $object, $obj );
    return unless scalar keys %$stats;
    $players->{$_} = 1 for keys %$stats;
    $object = $bucket->object(
        key => 'stats/' . $round . '_' . $stat . '.json',
        acl_short => 'public-read',
        content_type => 'application/json',
    );
    $obj = from_json( $object->get ) if $object->exists;
    delete $obj->{info};
    for my $p ( keys %$stats ) {
        my $row = $stats->{$p};
        unless( exists $obj->{$p} ) {
            $obj->{$p} = [{ t => $row->{t}, v => $row->{v} }];
        }
        else {
            my $vals = $obj->{$p};
            my $found;
            for ( reverse( 0 .. $#$vals ) ) {
                if( $vals->[$_]->{t} eq $row->{t} ) {
                    $vals->[$_]->{v} = $row->{v};
                    $found = 1;
                    last;
                }
            }
            if( not $found ) {
                push @$vals, { t => $row->{t}, v => $row->{v} };
            }
        }
    }
    my $max_v = 0;
    my $max_t = 0;
    my $p_max;
    for my $p ( keys %$obj ) {
        my $vals = $obj->{$p};
        $p_max = max( map { $_->{v} } @$vals );
        if( $p_max > $max_v ) {
            $max_v = $p_max;
        }
        $p_max = max( map { $_->{t} } @$vals );
        if( $p_max > $max_t ) {
            $max_t = $p_max;
        }
    }
    $obj->{info}->{round} = $round;
    $obj->{info}->{max_v} = $max_v;
    $obj->{info}->{max_t} = $max_t;
    $obj->{info}->{players} = get_player_info([ grep { $_ ne 'info' } keys %$obj ]);
    $obj->{info}->{name} = $name;

    $obj = to_json( $obj );
    $object->put( $obj );
    $object = $bucket->object(
        key => 'stats/' . $stat . '.json',
        acl_short => 'public-read',
        content_type => 'application/json',
    );
    $object->put( $obj );
}

$pidfile->remove();
say strftime('%Y-%m-%d %H:%M:%S', localtime), ' - End';
