class Assimilate::Extender
  attr_reader :domain, :idfield, :datestamp, :keyfield

  def initialize(args)
    @catalog = args[:catalog]
    @domain = args[:domain]
    @datestamp = args[:datestamp]
    @idfield = args[:idfield]
    @filename = args[:filename]
    @keyfield = args[:key]

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

  def <<(record)
    @seen ||= Hash.new(0)

    hash = record.to_hash
    key = hash[@idfield]
    data = hash.reject {|k,v| k == @keyfield}
    @seen[key] = data
    current_record = @baseline[key]
    if current_record
      if current_record[@keyfield] == data
        @noops << key
      else
        @changes << key
      end
    else
      @adds << key
    end
  end

  def stats
    {
      :baseline_count => @baseline.size,
      :final_count => @baseline.size + @adds.count,
      :distinct_ids => @seen.size,
      :adds_count => @adds.count,
      :updates_count => @changes.count,
      :unchanged_count => @noops.count
    }
  end

  # write all the changes to the catalog
  def commit
    # puts "ADDS"
    # puts @adds.inspect
    # puts "CHANGES"
    # puts @changes.inspect
    # puts "DONE"
    apply_inserts
    apply_updates
  end

  # an "insert" here means a record for which we have extended data
  # but does not appear in the current catalog, so we need to create
  # a stub entry.
  def apply_inserts
    @adds.each do |key|
      data = @seen[key]
      @catalog.catalog.insert(
        @catalog.domainkey => domain,
        idfield => key,
        keyfield => data
      )
    end
  end

  # "update" means store the extended data in the record (which must exist)
  def apply_updates
    @changes.each do |key|
      data = @seen[key]
      @catalog.catalog.update(
        {
          @catalog.domainkey => domain,
          idfield => key
        },
        {"$set" => {
            keyfield => data
          }
        }
      )
    end
  end


end
