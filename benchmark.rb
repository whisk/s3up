require 'lib/s3up.rb'
$stdout.sync = true


NUM = (ENV['NUM'] || 10).to_i
FILES = ARGV
s3 = S3up.new('aws.yml')

#methods = [:upload, :multipart_upload, :threaded_upload]
methods = [:threaded_upload]
compare_with_method = :upload

FILES.each do |filename|
  begin
    next unless File.exist?(filename)
    filesize = File.size(filename)
    t_total = {}
    print tmp = "Benchmarking #{filename} (#{filesize} bytes) "
    NUM.times do |i|
      print '.'
      methods.reverse.each do |method|
        t0 = Time.now.to_f
        s3.send(method, filename)
        t_total[method] ||= 0.0
        t_total[method] += Time.now.to_f - t0
      end
    end
  rescue Interrupt => e
    puts "Aborted by user"
    exit
  ensure
    print "\n"
    methods.each do |method|
      t_total[method] ||= 0
      t = t_total[method] / NUM
      ratio = (t_total[compare_with_method] / NUM) / t rescue 0
      speed = filesize / t / 1024 / 1024 rescue 0
      printf tmp = "#{method.to_s} avg. upload time: %0.2fs (%0.2fMb/s, %0.2fX)\n", t, speed, ratio
    end
    print '-' * 40 + "\n"
  end
end

