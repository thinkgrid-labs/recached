class Liyab < Formula
  desc "Blazing fast, multi-core drop-in replacement for Redis"
  homepage "https://github.com/YOUR_GITHUB_USERNAME/liyab"
  
  # When you release, you will update these lines with the actual version and URL
  url "https://github.com/YOUR_GITHUB_USERNAME/liyab/releases/download/v0.1.0/liyab-macos-arm64"
  version "0.1.0"
  sha256 "REPLACE_WITH_ACTUAL_SHA256"
  
  license "MIT"

  def install
    # Rename the downloaded binary to 'liyab-server' and install it into the Homebrew bin
    bin.install "liyab-macos-arm64" => "liyab-server"
  end

  # This allows users to run `brew services start liyab` to run it automatically in the background
  service do
    run opt_bin/"liyab-server"
    keep_alive true
    error_log_path var/"log/liyab.error.log"
    log_path var/"log/liyab.log"
  end

  test do
    # Simple test to verify the binary installed correctly
    system "#{bin}/liyab-server", "--version"
  end
end
