class Assimilate::Batch
  attr_reader :domain, :idfield, :datestamp

  def initialize(args)
    @catalog = args[:catalog]
    @domain = args[:domain]
    @datestamp = args[:datestamp]
    @idfield = args[:idfield]
    @filename = args[:filename]

    load_baseline

    @noops = []
    @changes = []
    @adds = []
    @deletes = []
  end

  def load_baseline
    stored_records = @catalog.catalog.find(@catalog.domainkey => @domain).to_a
    @baseline = stored_records.each_with_object({}) do |rec, h|
      key = rec[@idfield]
      if h.include?(key)
        raise Assimilate::CorruptDataError, "Duplicate records for key [#{key}] in domain [#{@domain}]"
      end
      h[key] = rec
    end
  end

  def stripped_record_for(key)
    @baseline[key] && @baseline[key].select {|k,v| k !~ /^_/}
  end

  def <<(record)
    @seen ||= Hash.new(0)

    hash = record.to_hash
    key = hash[@idfield]
    @seen[key] += 1
    current_record = stripped_record_for(key)
    if current_record
      if current_record == hash
        @noops << hash
      else
        @changes << hash
      end
    else
      @adds << hash
    end
  end

  # compute anything needed before we can write updates to permanent store
  # * find records that have been deleted
  def resolve
    @deleted_keys = @baseline.keys - @seen.keys
  end

  def stats
    resolve
    {
      :adds_count => @adds.count,
      :deletes_count => @deleted_keys.count,
      :updates_count => @changes.count,
      :unchanged_count => @noops.count
    }
  end

  # write the updates to the catalog
  def commit
    resolve
    record_batch
    apply_deletes
    apply_inserts
    apply_updates
  end

  def record_batch
    raise(Assimilate::DuplicateImportError, "duplicate batch") if @catalog.batches.find('domain' => @domain, 'datestamp' => @datestamp).to_a.any?
    @catalog.batches.insert({
      'domain' => @domain,
      'datestamp' => @datestamp,
      'filename' => @filename
      })
  end

  def apply_deletes
    @deleted_keys.each do |key|
      @catalog.catalog.update(
        {
          @catalog.domainkey => domain,
          idfield => key
        },
        {"$set" => {:_dt_removed => datestamp}}
    )
    end
  end

  INSERT_BATCH_SIZE = 1000 # default batch size for bulk loading into mongo

  def apply_inserts
    @adds.each_slice(INSERT_BATCH_SIZE) do |slice|
      # mongo insert can't handle CSV::Row objects, must be converted to regular hashes
      @catalog.catalog.insert(decorate(slice))
    end
  end

  def apply_updates
    @changes.each do |rec|
      @catalog.catalog.update(
        {
          @catalog.domainkey => domain,
          idfield => rec[idfield]
        },
        {"$set" => rec}
      )
    end
  end

  def decorate(records)
    records.map do |r|
      r[@catalog.domainkey] = @domain
      r.to_hash
    end
  end
end

class Assimilate::DuplicateImportError < StandardError
end
