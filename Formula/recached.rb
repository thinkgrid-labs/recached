class Recached < Formula
  desc "Blazing fast, multi-core drop-in replacement for Redis"
  homepage "https://github.com/YOUR_GITHUB_USERNAME/recached"
  
  # When you release, you will update these lines with the actual version and URL
  url "https://github.com/YOUR_GITHUB_USERNAME/recached/releases/download/v0.1.0/recached-macos-arm64"
  version "0.1.0"
  sha256 "REPLACE_WITH_ACTUAL_SHA256"
  
  license "MIT"

  def install
    # Rename the downloaded binary to 'recached-server' and install it into the Homebrew bin
    bin.install "recached-macos-arm64" => "recached-server"
  end

  # This allows users to run `brew services start recached` to run it automatically in the background
  service do
    run opt_bin/"recached-server"
    keep_alive true
    error_log_path var/"log/recached.error.log"
    log_path var/"log/recached.log"
  end

  test do
    # Simple test to verify the binary installed correctly
    system "#{bin}/recached-server", "--version"
  end
end
