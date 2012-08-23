require "mongo"
require "active_support/core_ext" # needed for Hash#symbolize_keys!
require "csv"

require_relative "assimilate/version"

require_relative "assimilate/catalog"
require_relative "assimilate/batch"
require_relative "assimilate/extender"

require_relative "assimilate/command"

module Assimilate
  def self.load(filename, opts = {})
    begin
      catalog = Catalog.new(:config => opts[:config])
      batcher = catalog.start_batch(opts.merge(:filename => filename))

      slurp(filename) do |rec|
        batcher << rec
      end
      if opts[:commit]
        batcher.commit
      else
        $stderr.puts "(suppressing data commit)"
      end
      batcher.stats
    # TODO explicit handling for Assimilate exceptions - when code is stable
    # rescue Assimilate::DuplicateImportError => e
    #   $stderr.puts e.message
    #   exit 1
    end
  end

  def self.extend_data(filename, opts = {})
    begin
      catalog = Catalog.new(:config => opts[:config])
      extender = catalog.extend_data(opts)
      slurp(filename) do |rec|
        extender << rec
      end
      if opts[:commit]
        extender.commit
      else
        $stderr.puts "(suppressing data commit)"

        if ENV['ASSIM_VERBOSE']
          $stderr.puts "UPDATES:"
          extender.changes.each do |recid|
            $stderr.puts "#{recid}: #{extender.seen[recid]}"
          end
        end
      end
      extender.stats
    end
  end

  def self.slurp(filename)
    headers = nil
    CSV.read(filename).each do |row|
      if !headers
        headers = row.to_a
      else
        raise "Row count mismatch: #{row} vs #{headers}" if row.count > headers.count
        hash = {}
        row.zip(headers) do |v,k|
          hash[k] = v.strip unless v.blank?
        end
        yield hash
      end
    end
  end
end
