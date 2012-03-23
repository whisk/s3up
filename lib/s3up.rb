require 'aws-sdk'
require 'digest/md5'

class S3up
  attr_reader :bucket

  def initialize(config_path)
    config = YAML.load_file(config_path)
    @bucket = AWS::S3.new(
      :access_key_id => config['access_key_id'],
      :secret_access_key => config['secret_access_key']).buckets[config['bucket_name']]
  end

  def upload(src, dst = nil, check_md5 = false)
    dst ||= File.basename(src)

    s3_file = bucket.objects[dst]
    s3_file.write(:file => src, :acl => :public_read)

    raise "S3: Error uploading file '#{src}' -> '#{dst}" unless s3_file.instance_of?(AWS::S3::S3Object)
    do_check_md5(src, s3_file) if check_md5

    s3_file
  end

  def multipart_upload(src, dst = nil, check_md5 = false)
    dst ||= File.basename(src)
    s3_file = bucket.objects[dst]
    
    src_io = File.open(src, 'rb')
    read_size = 0
    uploaded_size = 0
    parts = 0
    src_size = File.size(src)
    s3_file = s3_file.multipart_upload({:acl => :public_read}) do |upload|
      upload_threads = []
      mutex = Mutex.new
      max_threads = [config['threads_count'], (src_size.to_f / config['part_size']).ceil].min
      max_threads.times do |thread_number|
        upload_threads << (Thread.new do
          while (read_size < src_size)
            mutex.lock
            buff = src_io.readpartial(config['part_size'])
            read_size += buff.size
            part_number = parts += 1
            mutex.unlock
            
            upload.add_part :data => buff, :part_number => part_number
            uploaded_size += buff.size

            yield(uploaded_size.to_f / src_size) if block_given?
          end
        end)
      end
      upload_threads.each{|thread| thread.join}
    end
    src_io.close

    raise "S3: Error uploading file '#{src}' -> '#{dst}" unless s3_file.instance_of?(AWS::S3::S3Object)
    do_check_md5(src, s3_file) if check_md5

    s3_file
  end

  private

  def do_check_md5(file, s3_file)
    local = Digest::MD5.hexdigest(File.read(file))
    remote = s3_file.etag.gsub(/"/, '')
    raise "S3: MD5 check failed (expected #{local}, was #{remote})" unless local == remote
  end
end
