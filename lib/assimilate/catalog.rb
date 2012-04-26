class Assimilate::Catalog
  def initialize(args)
    # connect to mongo via :db
  end

  def start_batch(args)
    Assimilate::Batch.new(args.merge(:db => @db))
  end
end
