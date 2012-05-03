require "yaml"

# Catalog configuration:
#   db              name of mongo database
#   catalog         name of the catalog collection
#   batch           name of the batches collection (e.g. "files")
#   domain          key to use for specifying record domains (will be prefixed with _)
#   deletion_marker key to use to marker records that have disappeared from the source file
#
# Records in each catalog acquire the following internal attributes:
#   _id               Unique ID, assigned by mongo
#   [domain]          Domain key, specified with :domainkey attribute when initializing catalog
#   _dt_first_seen    Batch datestamp reference for when this record was first captured
#   _dt_last_seen     Batch datestamp reference for when this record was most recently affirmed
#   _dt_last_update   Batch datestamp reference for when this record was most recently altered
#   [deletion_marker] Batch datestamp reference for when this record was removed from input
#
# Inbound records must not have attributes named with leading underscores.
#
# A "domain" here is a namespace of identifiers.

class Assimilate::Catalog
  attr_reader :catalog, :config, :batches

  def initialize(args)
    @config = YAML.load(File.open(args[:config]))
    check_config

    @db = Mongo::Connection.new.db(@config[:db])
    @catalog = @db.collection(@config[:catalog])
    @batches = @db.collection(@config[:batch])
  end

  def check_config
    config.symbolize_keys!
    [:db, :catalog, :batch, :domain, :deletion_marker, :insertion_marker, :update_marker].each do |key|
      raise Assimilate::InvalidConfiguration, "missing required parameter: #{key}" unless config[key]
    end
    [:domain, :deletion_marker, :insertion_marker, :update_marker].each do |key|
      # enforce leading underscore on internal attributes
      config[key] = "_#{config[key]}" unless config[key] =~ /^_/
    end
  end

  def start_batch(args)
    Assimilate::Batch.new(args.merge(:catalog => self))
  end

  def extend_data(args)
    Assimilate::Extender.new(args.merge(:catalog => self))
  end

  def where(params)
    records = @catalog.find(params).to_a #.map {|rec| rec.select {|k,v| k !~ /^_/}}
    if records.count == 1
      records.first
    else
      records
    end
  end

  def active_count
    @catalog.find(config[:deletion_marker] => nil).count
  end
end

class Assimilate::InvalidConfiguration < StandardError
end
