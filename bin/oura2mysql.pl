#!/usr/bin/perl

use strict;
use warnings;

use Date::Parse qw(str2time);
use DateTime;
use DBI;
use Getopt::Long;
use JSON qw(encode_json);
use WWW::Oura::API;
use YAML qw(LoadFile);

my $path = './conf/oura2mysql.yml';
my $init = 0;
my $dry_run = 0;
my $date = DateTime->now->subtract(days => 1)->strftime("%Y-%m-%d");
my $token;

GetOptions(
    "conf=s"  => \$path,
    "init"    => \$init,
    "dry-run" => \$dry_run,
    "date=s"  => \$date,
    "token=s" => \$token,
);

die "No conf file path specified" unless $path;
die "Conf file $path does not exist" unless -e $path;
my ($conf) = LoadFile($path);

$token ||= $conf->{token} || $ENV{OURA_TOKEN};
die "No token provided" unless $token;

my $oura = WWW::Oura::API->new( token => $token );

my $dbh = get_dbh();

if ($init)
{
    init_db();
}
else
{
    sync();    
}

sub sync
{
    for my $endpoint ( keys %{ $conf->{endpoints} } )
    {
        my $res = $oura->api_call("usercollection/$endpoint", {
            start_date => $date,
            end_date   => $date,
        });

        for my $data_point (@{ $res->{data} || [] })
        {
            my @fields;
            my @insert_data;
            for my $field (@{ $conf->{endpoints}{$endpoint}{oura_fields} })
            {
                push @fields, $field->{name};
                my $data_val = $data_point->{$field->{name}};
                if ($field->{type} and $field->{type} eq 'datetime' and $data_val)
                {
                    # HACK FIXME
                    $data_val = DateTime->from_epoch( epoch => str2time( $data_val ) )->strftime("%Y-%m-%d %H:%M:%S");
                }
                # HACK FIXME
                if ($field->{name} eq 'day' and !$data_val)
                {
                    $data_val = $date;
                }
                push @insert_data, $data_val;
            }

            my $field_list = join(',', @fields);
            my $bind_list  = join(',', map { "?" } @fields);
            dbh_do($dbh, qq|
                INSERT INTO `$endpoint` ($field_list)
                VALUES($bind_list)
            |, @insert_data);
        }

        if (my $archive_table = $conf->{database}{full_archive_table})
        {
            my $sql = qq|
                INSERT INTO `$archive_table` (`endpoint`, `date`, `raw_data`)
                VALUES(?, ?, ?)
            |;
            dbh_do($dbh, $sql, $endpoint, $date, encode_json( $res->{data} ));
        }
    }
}

sub init_db
{
    for my $endpoint ( keys %{ $conf->{endpoints} } )
    {
        my $fields;
        for my $field (@{ $conf->{endpoints}{$endpoint}{oura_fields} })
        {
            my $sql_field_type = $field->{type} || 'decimal(16,6)';
            $sql_field_type = "VARCHAR(255)" if $sql_field_type eq 'string';

            $fields .= qq|, `$field->{name}` $sql_field_type|;
        }

        my $sql = qq|
            CREATE TABLE $endpoint (
                `id` int not null auto_increment
                $fields,
                PRIMARY KEY(id)
            );
        |;
        dbh_do($dbh, $sql);
    }

    if (my $archive_table = $conf->{database}{full_archive_table})
    {
        my $sql = qq|
            CREATE TABLE $archive_table (
               `id` int not null auto_increment,
                `endpoint` varchar(255),
                `date` date,
                `raw_data` text,
                primary key(id)
            );
        |;
        dbh_do($dbh, $sql);
    }
}

sub dbh_do
{
    my ($dbh, $sql, @binds) = @_;
    if ($dry_run)
    {
        warn $sql;
    }
    else
    {
        $dbh->do( $sql, undef, @binds ) or die $!;
    }
}

sub get_dbh
{
    return DBI->connect(
        "DBI:mysql:database=$conf->{database}{db_name};host=$conf->{database}{server}",
        $conf->{database}{username},
        $conf->{database}{password},
    ) || die $!;
}

