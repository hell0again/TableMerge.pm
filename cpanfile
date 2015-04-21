requires 'perl', '5.008001';

requires("Algorithm::Combinatorics");
requires("Digest::SHA1");
requires("Encode");
requires("File::Basename");
requires("File::Path");
requires("File::Slurp");
requires("File::Temp");
requires("Getopt::Long");
requires("JSON::XS");
requires("List::Compare");
requires("List::Permutor");
requires("Log::Log4perl");
requires("Text::CSV_XS");
requires("Text::Levenshtein::XS");
requires("UNIVERSAL::require");

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::Exception';
};

