# frozen_string_literal: true

gem "aws-sdk-s3", "~> 1.48"

require "aws-sdk-s3"
require "active_support/core_ext/numeric/bytes"

module ActiveStorage
  # = Active Storage \S3 \Service
  #
  # Wraps the Amazon Simple Storage Service (S3) as an Active Storage service.
  # See ActiveStorage::Service for the generic API documentation that applies to all services.
  class Service::S3Service < Service
    attr_reader :bucket, :client, :default_digest_algorithm
    attr_reader :multipart_upload_threshold, :upload_options

    SUPPORTED_CHECKSUM_ALGORITHMS = [
      :CRC32,
      :CRC32c,
      :MD5,
      :SHA1,
      :SHA256,
      :CRC64NVMe
    ]

    def initialize(bucket:, upload: {}, public: false, default_digest_algorithm: :MD5, **options)
      @client = Aws::S3::Resource.new(**options)
      @bucket = @client.bucket(bucket)

      @multipart_upload_threshold = upload.delete(:multipart_threshold) || 100.megabytes
      @public = public

      @upload_options = upload
      @upload_options[:acl] = "public-read" if public?
      @default_digest_algorithm = default_digest_algorithm.to_sym
      raise ActiveStorage::UnsupportedChecksumError unless SUPPORTED_CHECKSUM_ALGORITHMS.include?(@default_digest_algorithm)
    end

    def upload(key, io, checksum: nil, filename: nil, content_type: nil, disposition: nil, custom_metadata: {}, **)
      instrument :upload, key: key, checksum: checksum do
        content_disposition = content_disposition_with(filename: filename, type: disposition) if disposition && filename

        if io.size < multipart_upload_threshold
          upload_with_single_part key, io, checksum: checksum, content_type: content_type, content_disposition: content_disposition, custom_metadata: custom_metadata
        else
          upload_with_multipart key, io, content_type: content_type, content_disposition: content_disposition, custom_metadata: custom_metadata
        end
      end
    end

    def download(key, &block)
      if block_given?
        instrument :streaming_download, key: key do
          stream(key, &block)
        end
      else
        instrument :download, key: key do
          object_for(key).get.body.string.force_encoding(Encoding::BINARY)
        rescue Aws::S3::Errors::NoSuchKey
          raise ActiveStorage::FileNotFoundError
        end
      end
    end

    def download_chunk(key, range)
      instrument :download_chunk, key: key, range: range do
        object_for(key).get(range: "bytes=#{range.begin}-#{range.exclude_end? ? range.end - 1 : range.end}").body.string.force_encoding(Encoding::BINARY)
      rescue Aws::S3::Errors::NoSuchKey
        raise ActiveStorage::FileNotFoundError
      end
    end

    def delete(key)
      instrument :delete, key: key do
        object_for(key).delete
      end
    end

    def delete_prefixed(prefix)
      instrument :delete_prefixed, prefix: prefix do
        bucket.objects(prefix: prefix).batch_delete!
      end
    end

    def exist?(key)
      instrument :exist, key: key do |payload|
        answer = object_for(key).exists?
        payload[:exist] = answer
        answer
      end
    end

    def url_for_direct_upload(key, expires_in:, content_type:, content_length:, checksum:, checksum_algorithm:, custom_metadata: {}, **kwargs)
      instrument :url, key: key do |payload|
        generated_url = object_for(key).presigned_url :put, expires_in: expires_in.to_i,
          content_type: content_type, content_length: content_length, **s3_sdk_upload_params(checksum),
          metadata: custom_metadata, whitelist_headers: ["content-length"], **upload_options

        payload[:url] = generated_url

        generated_url
      end
    end

    def headers_for_direct_upload(key, content_type:, checksum:, filename: nil, disposition: nil, custom_metadata: {}, **)
      content_disposition = content_disposition_with(type: disposition, filename: filename) if filename

      { "Content-Type" => content_type, **s3_http_headers_for_direct_upload(checksum), "Content-Disposition" => content_disposition, **custom_metadata_headers(custom_metadata) }
    end

    def compose(source_keys, destination_key, filename: nil, content_type: nil, disposition: nil, custom_metadata: {})
      content_disposition = content_disposition_with(type: disposition, filename: filename) if disposition && filename

      object_for(destination_key).upload_stream(
        content_type: content_type,
        content_disposition: content_disposition,
        part_size: MINIMUM_UPLOAD_PART_SIZE,
        metadata: custom_metadata,
        **upload_options
      ) do |out|
        source_keys.each do |source_key|
          stream(source_key) do |chunk|
            IO.copy_stream(StringIO.new(chunk), out)
          end
        end
      end
    end

    private
      def private_url(key, expires_in:, filename:, disposition:, content_type:, **client_opts)
        object_for(key).presigned_url :get, expires_in: expires_in.to_i,
          response_content_disposition: content_disposition_with(type: disposition, filename: filename),
          response_content_type: content_type, **client_opts
      end

      def public_url(key, **client_opts)
        object_for(key).public_url(**client_opts)
      end


      MAXIMUM_UPLOAD_PARTS_COUNT = 10000
      MINIMUM_UPLOAD_PART_SIZE   = 5.megabytes

      def upload_with_single_part(key, io, checksum: nil, content_type: nil, content_disposition: nil, custom_metadata: {})
        object_for(key).put(body: io, **s3_sdk_upload_params(checksum), content_type: content_type, content_disposition: content_disposition, metadata: custom_metadata, **upload_options)
      rescue Aws::S3::Errors::BadDigest
        raise ActiveStorage::IntegrityError
      rescue Aws::S3::Errors::InvalidRequest => e
        raise ActiveStorage::IntegrityError if e.message.match?(/Value for x-amz-checksum-.* header is invalid./)
      end

      def upload_with_multipart(key, io, content_type: nil, content_disposition: nil, custom_metadata: {})
        part_size = [ io.size.fdiv(MAXIMUM_UPLOAD_PARTS_COUNT).ceil, MINIMUM_UPLOAD_PART_SIZE ].max

        object_for(key).upload_stream(content_type: content_type, content_disposition: content_disposition, part_size: part_size, metadata: custom_metadata, **upload_options) do |out|
          IO.copy_stream(io, out)
        end
      end


      def object_for(key)
        bucket.object(key)
      end

      # Reads the object for the given key in chunks, yielding each to the block.
      def stream(key)
        object = object_for(key)

        chunk_size = 5.megabytes
        offset = 0

        raise ActiveStorage::FileNotFoundError unless object.exists?

        while offset < object.content_length
          yield object.get(range: "bytes=#{offset}-#{offset + chunk_size - 1}").body.string.force_encoding(Encoding::BINARY)
          offset += chunk_size
        end
      end

      def custom_metadata_headers(metadata)
        metadata.transform_keys { |key| "x-amz-meta-#{key}" }
      end

      def s3_sdk_upload_params(checksum, algorithm = :MD5)
        return {} unless checksum
        return { content_md5: checksum } if algorithm == :MD5

        {
          checksum_algorithm: algorithm,
          "checksum_#{algorithm.downcase}": checksum
        }
      end

      def s3_http_headers_for_direct_upload(checksum, algorithm = :MD5)
        return {} unless checksum
        return { "Content-MD5" => checksum } if algorithm == :MD5

        { "x-amz-checksum-#{algorithm.downcase}" => checksum }
      end

      def sha1
        OpenSSL::Digest::SHA1
      end

      def sha256
        OpenSSL::Digest::SHA256
      end

      def crc32
        return @crc32_class if @crc32_class
        begin
          require "digest/crc32"
        rescue LoadError
          raise LoadError, 'digest/crc32 not loaded. Please add `gem "digest-crc"` to your gemfile.'
        end
        @crc32_class = Digest::CRC32
      end

      def crc32c
        return @crc32c_class if @crc32c_class
        begin
          require "digest/crc32c"
        rescue LoadError
          raise LoadError, 'digest/crc32c not loaded. Please add `gem "digest-crc"` to your gemfile.'
        end
        @crc32c_class = Digest::CRC32c
      end

      def crc64nvme
        return @crc64nvme_class if @crc64nvme_class
        begin
          require "digest/crc64nvme"
        rescue LoadError
          raise LoadError, 'digest/crc64nvme not loaded. Please add `gem "digest-crc"` to your gemfile.'
        end
        @crc64nvme_class = Digest::CRC64NVMe
      end
  end
end
