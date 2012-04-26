require "yaml"

class Assimilate::Catalog
  attr_reader :catalog, :batches, :domainkey

  def initialize(args)
    @config = YAML.load(File.open(args[:config]))

    @db = Mongo::Connection.new.db(@config['db'])
    @catalog = @db.collection(@config['catalog'])
    @batches = @db.collection(@config['batch'])
    @domainkey = @config['domain']
    @domainkey = "_#{@domainkey}" unless @domainkey =~ /^_/ # enforce leading underscore on internal attributes
  end

  def start_batch(args)
    Assimilate::Batch.new(args.merge(:catalog => self))
  end

  def where(params)
    @catalog.find(params).first.delete_if {|k,v| k == '_id'}
  end
end
