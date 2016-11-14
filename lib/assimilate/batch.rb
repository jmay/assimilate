class Assimilate::Batch
  attr_reader :domain, :idfield, :datestamp

  def initialize(args)
    @catalog = args[:catalog]
    @domainkey = @catalog.config[:domain]

    @domain = args[:domain]
    @datestamp = args[:datestamp]
    @idfield = args[:idfield]
    @filename = args[:filename]

    @subset = args[:subset]
    @suppress_deletes = args[:nodeletes]

    load_baseline

    @noops = []
    @changes = {}
    @adds = []
    @deletes = []
    @resolved = false
  end

  def prime(fieldnames)
    @fields = fieldnames
  end

  def load_baseline
    stored_records = @catalog.catalog.find(@domainkey => @domain, @idfield => {"$exists" => 1}).to_a
    @baseline = stored_records.each_with_object({}) do |rec, h|
      key = rec[@idfield]
      if h.include?(key)
        raise Assimilate::CorruptDataError, "Duplicate records for key [#{key}] in #{@domainkey} [#{@domain}]"
      end
      h[key] = rec
    end
  end

  # The stripped record contains only the data values from the source (no internal values with leading underscores).
  # Any nil values are ignored; these should not be stored but if they do appear in the catalog then don't want
  # to include them when comparing new records vs. old.
  def stripped_record_for(key)
    if @subset
      @baseline[key] && @baseline[key].select {|k,v| @fields.include?(k)}
    else
      @baseline[key] && @baseline[key].select {|k,v| k !~ /^_/ && !v.nil?}
    end
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
        @changes[key] = deltas(current_record, hash)
      end
    else
      @adds << hash
    end
  end

  def deltas(h1,h2)
    (h1.keys | h2.keys).each_with_object({}) {|k,h| h[k] = h2[k] if h1[k] != h2[k]}
  end

  # compute anything needed before we can write updates to permanent store
  # * find records that have been deleted
  def resolve
    if !@resolved
      @deleted_keys = (@baseline.keys - @seen.keys).reject {|k| @baseline[k][@catalog.config[:deletion_marker]]}

      @updated_field_counts = @changes.each_with_object(Hash.new(0)) do |(_,diffs),h|
        # key = rec[idfield]
        # diffs = deltas(stripped_record_for(key), rec)
        diffs.keys.each do |f|
          h[f] += 1
        end
      end

      @resolved = true
    end
  end

  def stats
    resolve
    {
      :baseline_count => @baseline.size,
      :final_count => @baseline.size + @adds.count,
      :adds_count => @adds.count,
      :new_ids => @adds.map {|rec| rec[idfield]},
      :deletes_count => @deleted_keys.count,
      :deleted_ids => @deleted_keys,
      :updates_count => @changes.size,
      :updated_ids => @changes.keys,
      :unchanged_count => @noops.count,
      :updated_fields => @updated_field_counts
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
    # don't want leading underscore on attributes in the batches table
    dkey = @domainkey.gsub(/^_/,'')

    raise(Assimilate::DuplicateImportError, "duplicate batch for datestamp #{datestamp}") if @catalog.batches.find(dkey => @domain, 'datestamp' => @datestamp).to_a.any?
    raise(Assimilate::DuplicateImportError, "duplicate batch for file #{@filename}") if @catalog.batches.find(dkey => @domain, 'filename' => @filename).to_a.any?

    @catalog.batches.insert_one({
      dkey => @domain,
      'datestamp' => @datestamp,
      'filename' => @filename
      })
  end

  def apply_deletes
    unless @suppress_deletes
      @deleted_keys.each do |key|
        @catalog.catalog.update_one(
          {
            @domainkey => domain,
            idfield => key
          },
          {
            "$set" => {@catalog.config[:deletion_marker] => datestamp}
          }
      )
      end
    end
  end

  INSERT_BATCH_SIZE = 1000 # default batch size for bulk loading into mongo

  def apply_inserts
    @adds.each_slice(INSERT_BATCH_SIZE) do |slice|
      # mongo insert can't handle CSV::Row objects, must be converted to regular hashes
      @catalog.catalog.insert_many(decorate(slice))
    end
  end

  def apply_updates
    marker = @catalog.config[:update_marker]
    @changes.each do |key, diffs|
      @catalog.catalog.update_one(
        {
          @domainkey => domain,
          idfield => key
        },
        {
          "$set" => diffs.merge(marker => datestamp)
        }
      )
    end
  end

  def decorate(records)
    marker = @catalog.config[:insertion_marker]
    records.map do |r|
      r[@domainkey] = @domain
      r[marker] = datestamp
      r.to_hash
    end
  end
end

class Assimilate::DuplicateImportError < StandardError
end

class Assimilate::CorruptDataError < StandardError
end
