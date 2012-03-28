require 'aws-sdk'

class S3up
  attr_reader :bucket
  attr_accessor :config

  def initialize(config_path)
    @config = YAML.load_file(config_path)
    @bucket = AWS::S3.new(
      :access_key_id => config['access_key_id'],
      :secret_access_key => config['secret_access_key']).buckets[config['bucket_name']]
  end

  # простая закачка
  def upload(src, dst = nil)
    dst ||= File.basename(src)

    s3_file = bucket.objects[dst]
    s3_file.write(:file => src, :acl => :public_read)

    raise "S3: Error uploading file '#{src}' -> '#{dst}" unless s3_file.instance_of?(AWS::S3::S3Object)

    s3_file
  end

  # закачка частями
  def multipart_upload(src, dst = nil)
    dst ||= File.basename(src)
    s3_file = bucket.objects[dst]

    # открываем локальный src файл для чтения
    src_io = File.open(src, 'rb')
    # несколько полезных счетчиков
    read_size = 0
    uploaded_size = 0
    parts = 0
    # размер файла пригодится
    src_size = File.size(src)
    # начинаем закачку частями
    s3_file = s3_file.multipart_upload({:acl => :public_read}) do |upload|
      while read_size < src_size
        # считаем последовательность байт заданного размера из файла в буфер
        buff = src_io.readpartial(config['part_size'])
        # увеличим счетчики
        read_size += buff.size
        part_number = parts += 1

        # собственно, закачаем считанные данные на S3
        upload.add_part :data => buff, :part_number => part_number
        uploaded_size += buff.size

        # вызовем блок и передадим ему информацию о прогрессе закачки
        yield(uploaded_size.to_f / src_size) if block_given?
      end
    end
    # закроем исходный файл
    src_io.close

    raise "S3: Error uploading file '#{src}' -> '#{dst}" unless s3_file.instance_of?(AWS::S3::S3Object)

    s3_file
  end

  # многопоточная закачка частями
  def threaded_upload(src, dst = nil, check_md5 = false)
    dst ||= File.basename(src)
    s3_file = bucket.objects[dst]
    
    src_io = File.open(src, 'rb')
    read_size = 0
    uploaded_size = 0
    parts = 0
    src_size = File.size(src)
    s3_file = s3_file.multipart_upload({:acl => :public_read}) do |upload|
      # заведем массив для сохранения информации о потоках
      upload_threads = []
      # создадим мьютекс (или семафор), чтобы избежать “гонок”
      mutex = Mutex.new
      # число потоков - не больше максимально разрешенного значения
      max_threads = [config['threads_count'], (src_size.to_f / config['part_size']).ceil].min
      # в цикле создадим требуемое количество потоков
      max_threads.times do |thread_number|
        upload_threads << (Thread.new do
          # мы в свежесозданном потоке
          while true
            # входим в участок кода, в котором одновременно может находится только один поток
            # выставляем блокировку
            mutex.lock
            # прерываем цикл, если все данные уже прочитаны из файла
            break unless read_size < src_size
            # считаем последовательность байт заданного размера из файла в буфер
            buff = src_io.readpartial(config['part_size'])
            # увеличим счетчики
            read_size += buff.size
            part_number = parts += 1
            # снимаем блокировку
            mutex.unlock

            # собственно, закачаем считанные данные на S3
            upload.add_part :data => buff, :part_number => part_number

            # увеличение счетчика
            mutex.lock
            uploaded_size += buff.size
            mutex.unlock

            # вызовем блок и передадим ему информацию о прогрессе закачки
            yield(uploaded_size.to_f / src_size) if block_given?
          end
        end)
      end
      # дожидаемся завершения работы всех потоков
      upload_threads.each{|thread| thread.join}
    end
    src_io.close

    raise "S3: Error uploading file '#{src}' -> '#{dst}" unless s3_file.instance_of?(AWS::S3::S3Object)

    s3_file
  end

end
