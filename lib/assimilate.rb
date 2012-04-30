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
    catalog = Catalog.new(:config => opts[:config])
    batcher = catalog.start_batch(opts)

    @records = CSV.read(filename, :headers => true)
    @records.each do |rec|
      batcher << rec
    end
    batcher.commit
    batcher.stats
  end
end
