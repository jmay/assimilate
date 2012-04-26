class Assimilate::Batch
  def initialize(args)
    @catalog = args[:catalog]
    load_baseline

    @noops = []
    @changes = []
    @adds = []
    @deletes = []
  end

  def load_baseline
    @baseline = {}
  end

  def <<(hash)
    key = hash[@idfield]
    if current = @baseline[key]
      if current == hash
        @noops << hash
      else
        @changes << hash
      end
    else
      @adds << hash
    end
  end

  def stats
    {
      :adds_count => @adds.count,
      :deletes_count => @deletes.count,
      :updates_count => @changes.count,
      :unchanged_count => @noops.count
    }
  end

  # write the updates to the catalog
  def commit
    record_batch
    apply_deletes
    apply_inserts
    apply_updates
  end

  def record_batch
    raise "duplicate batch"
    # @catalog.batches.
  end

end
