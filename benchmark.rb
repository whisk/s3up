require 'lib/s3up.rb'
$stdout.sync = true


NUM = 10
FILES = ['1k.dat', '64k.dat', '256k.dat', '512k.dat', '1m.dat', '10m.dat', '50m.dat']
s3 = S3up.new('aws.yml')

FILES.each do |filename|
  next unless File.exist?(filename)
  filesize = File.size(filename)
  t_total = {}
  print tmp = "Benchmarking #{filename} (#{filesize} bytes) "
  NUM.times do |i|
    print '.'
    [:upload, :multipart_upload].reverse.each do |method|
      t0 = Time.now.to_f
      s3.send(method, filename, nil)
      t_total[method] ||= 0.0
      t_total[method] += Time.now.to_f - t0
    end
  end
  print "\n"
  t_total.each do |method, t|
    printf tmp = "#{method.to_s} avg. upload time: %0.2fs (%0.2fMb/s)\n", t / NUM, filesize / (t / NUM) / 1024 / 1024
  end
  print '-' * 40 + "\n"
end

