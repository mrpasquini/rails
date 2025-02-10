# frozen_string_literal: true

require "active_support/core_ext/object/try"

# Provides asynchronous mirroring of directly-uploaded blobs.
class ActiveStorage::MirrorJob < ActiveStorage::BaseJob
  queue_as { ActiveStorage.queues[:mirror] }

  discard_on ActiveStorage::FileNotFoundError
  retry_on ActiveStorage::IntegrityError, attempts: 10, wait: :polynomially_longer

  def perform(key, checksum:, checksum_algorithm: :MD5)
    ActiveStorage::Blob.service.try(:mirror, key, checksum: checksum, checksum_algorithm: checksum_algorithm)
  end
end
