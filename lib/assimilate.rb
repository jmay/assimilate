require "mongo"
require "active_support/core_ext" # needed for Hash#diff
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
      batcher = catalog.start_batch(opts)

      @records = CSV.read(filename, :headers => true)
      @records.each do |rec|
        batcher << rec
      end
      if opts[:commit]
        batcher.commit
      else
        $stderr.puts "suppressing data commit"
      end
      batcher.stats
    # TODO explicit handling for Assimilate exceptions - when code is stable
    # rescue Assimilate::DuplicateImportError => e
    #   $stderr.puts e.message
    #   exit 1
    end
  end
end
