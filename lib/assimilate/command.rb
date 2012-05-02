require "optparse"

class Assimilate::Command
  attr_reader :command, :options

  def initialize
    @options = {}
    @parser = OptionParser.new do |opts|
      opts.banner = "Usage: assimilate [command] [options]"

      opts.on("--config FILENAME", String, "Catalog database configuration file") do |f|
        @options[:config] = f
      end

      opts.on("--id FIELDNAME", String, "Field name to be used for record identifier") do |f|
        @options[:idfield] = f
      end

      opts.on("--commit", "Commit changes to database") do
        @options[:commit] = true
      end

      opts.on("--key FIELDNAME", String, "(*extend* only) Hash key to store extended attributes under") do |f|
        @options[:key] = f
      end

      opts.on("--datestamp DATESTRING", String, "(*load* only) Datestamp to record for file batch operation") do |s|
        @options[:datestamp] = s
      end

      opts.on("--domain STRING", String, "Domain value to apply to each record") do |s|
        @options[:domain] = s
      end
    end
  end

  def parse(argv = ARGV)
    @command = argv.shift
    filenames = @parser.parse(argv)

    raise OptionParser::MissingArgument, "missing config" unless options[:config]
    raise OptionParser::MissingArgument, "missing idfield" unless options[:idfield]
    raise OptionParser::MissingArgument, "missing domain" unless options[:domain]
    raise "missing filename" unless filenames.any?

    # argv remnants are filenames
    [@command, @options, filenames]
  end

  def execute(command, options, filenames = nil)
    filename = filenames.first

    case command
    when 'load'
      raise OptionParser::MissingArgument, "missing datestamp" unless options[:datestamp]

      results = Assimilate.load(filename, options)
      logmessage(command, options, results)

    when 'extend'
      raise OptionParser::MissingArgument, "missing keyfield" unless options[:key]

      results = Assimilate.extend_data(filename, options)
      logmessage(command, options, results)


    else
      raise "unknown command #{command}"
    end
  end

  def logmessage(command, options, results)
    $stderr.puts <<-EOT
* assimilate #{command} (#{options.keys.join(', ')})
EOT

    case command
    when 'load'
      $stderr.puts <<-EOT
    Original record count: #{results[:baseline_count]}
       Final record count: #{results[:final_count]}
        Unchanged records: #{results[:unchanged_count]}
              New records: #{results[:adds_count]} (#{results[:new_ids].take(10).join(',')})
                  Deletes: #{results[:deletes_count]}
                  Updates: #{results[:updates_count]}
EOT
      if results[:updated_fields].any?
        $stderr.puts <<-EOT
            Counts by field:
EOT
        results[:updated_fields].each do |k,v|
          $stderr.puts <<-EOT
                        #{k}: #{v}
EOT
        end
      end
    when 'extend'
      $stderr.puts <<-EOT
    Original record count: #{results[:baseline_count]}
       Final record count: #{results[:final_count]}
          New identifiers: #{results[:adds_count]} #{options[:idfield]} (#{results[:new_ids].take(10).join(',')})
             Distinct ids: #{results[:distinct_ids]}
        Unchanged records: #{results[:unchanged_count]}
                  Updates: #{results[:updates_count]}
EOT
      if results[:updated_fields].any?
        results[:updated_fields].each do |k,v|
          $stderr.puts <<-EOT
                        #{options[:key]}.#{k}: #{v}
EOT
        end
      end
    else
      $stderr.puts results.inspect
    end
  end
end
