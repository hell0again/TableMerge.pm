requires 'perl', '5.008001';

requires("Digest::SHA1");
requires("Encode");
requires("File::Basename");
requires("File::Path");
requires("File::Slurp");
requires("File::Temp");
requires("Getopt::Long");

requires("Text::CSV_XS");
requires("JSON::XS");

on 'test' => sub {
    requires 'Test::More', '0.98';
};

