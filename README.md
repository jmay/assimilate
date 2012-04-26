# Assimilate

Ingest updates from CSV and apply to set of persistent hashes

## Installation

Add this line to your application's Gemfile:

    gem 'assimilate'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install assimilate

## Usage

    assimilate --config repo.yml filename

or

    > require 'assimilate'
    > catalog = Assimilate::Catalog.new(:config => configfile)
    > catalog.start_batch(:filename => filename, :datestamp => datestamp, :idfield => idfield)

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

