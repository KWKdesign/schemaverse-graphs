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

my $last_tic = eval{ do 'last_tic' } || 0;
my $cur_tic = $dbh->selectrow_array(q/
    select last_value from tic_seq;
/);
my $last_round = eval{ do 'last_round' } || 0;
my $round = $dbh->selectrow_array(q/
    select last_value from round_seq;
/);
if ( ( $round > $last_round ) or ( $cur_tic > $last_tic ) ) {
    my $fh;
    open $fh, '>', 'last_round' or die $!;
    print $fh $last_round;
    close $fh;
    open $fh, '>', 'last_tic' or die $!;
    print $fh $cur_tic;
    close $fh;
}
else {
    die 'Not a new tic';
}


say strftime('%Y-%m-%d %H:%M:%S', localtime), ' - Start ' . $round;

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

$name = 'Planets';
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

$name = 'Planets Conquered';
$stats = get_stats('planets_conquered');
update_obj('planets_conquered');

$name = 'Planets Lost';
$stats = get_stats('planets_lost');
update_obj('planets_lost');

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

$name = 'Ships Living';
$stats = $dbh->selectall_hashref(q/
    select player_id::text p, ships_built - ships_lost v,
    ( select last_value from tic_seq ) t,
    ( select last_value from round_seq ) r
    from current_player_stats
    where ships_built - ships_lost > 0
    and player_id in (
        select id from player_list
        where 1=1
        and rgb is not null
        and symbol is not null
    )
    ;     
/, 'p');
update_obj('ships_living');


sub get_stats {
    # check_lock();
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
    # say $stat;
    my ( $object, $obj );
    
    my $bucket;
    if( defined $config->{s3_backend} and $config->{s3_backend} == 1 and eval "require Net::Amazon::S3" ) {
        my $s3 = Net::Amazon::S3->new({
            aws_access_key_id		=> $config->{aws_access_key_id},
            aws_secret_access_key	=> $config->{aws_secret_access_key},
        });
        my $c = Net::Amazon::S3::Client->new( s3 => $s3 );
        $bucket = $c->bucket( name => $config->{bucket_name} );
        $object = $bucket->object(
            key => 'stats/' . $round . '_' . $stat . '.json',
            acl_short => 'public-read',
            content_type => 'application/json',
        );
        $obj = from_json( $object->get ) if $object->exists
    }
    if( defined $config->{write_file} and $config->{write_file} == 1 ) {
        if( -e $config->{path} . $round . '_' . $stat . '.json' ) {
            my $json = '';
            {
                local $/;
                open my $fh, '<', $config->{path} . $round . '_' . $stat . '.json';
                $json = <$fh>;                
            }
            $obj = from_json( $json );
        }
    }
    delete $obj->{info};
    my $max_v = 0;
    my $max_t = 0;
    if( scalar keys %$stats ) {
        $players->{$_} = 1 for keys %$stats;
        
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
    }
    if( scalar keys %$obj ) {
        my $seen = {};
        $seen->{$_}++ for keys %$obj;
        $seen->{$_}++ for keys %$stats;
        for my $p ( grep { $seen->{$_} == 1 } keys %$seen ) {
            my $vals = $obj->{$p};
            if( $vals->[-1]->{v} != 0 ) {
                push @$vals, { t => $cur_tic, v => 0 };
            }
        }
        $obj->{info}->{players} = get_player_info([ keys %$obj ]);
    }    
    $obj->{info}->{round} = $round;
    $obj->{info}->{name} = $name;
    $obj->{info}->{max_v} = $max_v;
    $obj->{info}->{max_t} = $max_t;

    $obj = to_json( $obj );
    if( defined $config->{s3_backend} and $config->{s3_backend} == 1 and eval "require Net::Amazon::S3" ) {
        $object->put( $obj );
        $object = $bucket->object(
            key => 'stats/' . $stat . '.json',
            acl_short => 'public-read',
            content_type => 'application/json',
        );
        $object->put( $obj );
        # say $object->key;
    }
    if( defined $config->{write_file} and $config->{write_file} == 1 ) {
        my $fh;
        open $fh, '>', $config->{path} . $stat . '.json' or die $!;
        print $fh $obj;
        close $fh;
        open $fh, '>', $config->{path} . $round . '_' . $stat . '.json' or die $!;
        print $fh $obj;
        close $fh;
    }
    check_lock(); # safe to stop, check the lock
}

$pidfile->remove();
say strftime('%Y-%m-%d %H:%M:%S', localtime), ' - End';
