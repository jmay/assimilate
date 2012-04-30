require "yaml"

# Records in each catalog acquire the following internal attributes:
#   _id             Unique ID, assigned by mongo
#   _[domain]       Domain key, specified with :domainkey attribute when initializing catalog
#   _dt_first_seen  Batch datestamp reference for when this record was first captured
#   _dt_last_seen   Batch datestamp reference for when this record was most recently affirmed
#   _dt_last_update Batch datestamp reference for when this record was most recently altered
#   _dt_removed     Batch datestamp reference for when this record was removed from input
#
# Inbound records must not have attributes named with leading underscores.
#
# A "domain" here is a namespace of identifiers.

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

  def extend_data(args)
    Assimilate::Extender.new(args.merge(:catalog => self))
  end

  def where(params)
    @catalog.find(params).first.select {|k,v| k !~ /^_/}
  end

  def active_count
    @catalog.find("_dt_removed" => nil).count
  end
end
