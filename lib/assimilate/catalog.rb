require "yaml"

class Assimilate::Catalog
  attr_reader :catalog, :batches

  def initialize(args)
    @config = YAML.load(File.open(args[:config]))

    @db = Mongo::Connection.new.db(@config['db'])
    @catalog = @db.collection(@config['catalog'])
    @batches = @db.collection(@config['batch'])
  end

  def start_batch(args)
    Assimilate::Batch.new(args.merge(:catalog => self))
  end

  def where(params)
    
  end
end
