require 'lib/s3up.rb'
$stdout.sync = true


NUM = 10
FILES = ['1k.dat', '64k.dat', '256k.dat', '512k.dat', '1m.dat', '10m.dat', '50m.dat']
s3 = S3up.new('aws.yml')

[:upload, :multipart_upload].reverse.each do |method|
  print tmp = "Benchmarking method: #{method.to_s}\n"
  print "=" * tmp.size + "\n"
  FILES.each do |filename|
    next unless File.exist?(filename)
    t_total = 0.0
    filesize = File.size(filename)
    print "Benchmarking #{filename} (#{filesize} bytes) "
    NUM.times do |i|
      print '.'
      t0 = Time.now.to_f
      s3.upload(filename, nil)
      t_total += Time.now.to_f - t0
    end
    print "\n"
    printf tmp = "Avg. upload time: %0.4fs (%0.4fMb/s)\n", t_total / NUM, filesize / (t_total / NUM) / 1024 / 1024
    print "-" * tmp.size + "\n"
  end
end

