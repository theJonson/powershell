<#
.SYNOPSIS
    Synchronize local folders to Backblaze B2 object storage
.DESCRIPTION
    Syncs local files to B2 bucket, only uploading new or modified files
    Requires PowerShell 7+ for optimal performance
.PARAMETER ApplicationKeyId
    Your B2 application key ID
.PARAMETER ApplicationKey
    Your B2 application key
.PARAMETER BucketName
    The name of the B2 bucket to sync to
.PARAMETER LocalPath
    Local folder path to sync
.PARAMETER RemotePrefix
    Remote folder prefix in B2 (optional, defaults to root)
.PARAMETER ExcludeExtensions
    Array of file extensions to exclude (e.g., @(".tmp", ".log", ".cache"))
.PARAMETER ExcludeFolders
    Array of folder patterns to exclude using wildcards (e.g., @("*node_modules*", "*\.git*", "temp*"))
.PARAMETER BrevoApiKey
    Brevo (Sendinblue) API key for sending email notifications
.PARAMETER EmailTo
    Email address(es) to send the summary report to (can be array for multiple recipients)
.PARAMETER EmailFrom
    Email address to send from (must be verified in Brevo)
.PARAMETER EmailFromName
    Display name for the sender (default: "B2 Backup System")
.PARAMETER LargeFileThreshold
    File size threshold for using streaming upload (default: 2GB)
.PARAMETER HashCacheFile
    Path to store hash cache for faster subsequent syncs (e.g., "C:\Temp\b2_hash_cache.json")
.PARAMETER DeleteRemote
    If specified, deletes remote files that don't exist locally
.PARAMETER ShowErrors
    If specified, shows detailed error messages and diagnostic output
.EXAMPLE
    .\Sync-ToB2.ps1 -LocalPath "C:\MyFolder"
.EXAMPLE
    .\Sync-ToB2.ps1 -LocalPath "C:\MyFolder" -ExcludeExtensions @(".tmp", ".log", ".bak")
.EXAMPLE
    .\Sync-ToB2.ps1 -LocalPath "C:\MyFolder" -ExcludeFolders @("*node_modules*", "*\.git*", "temp*")
.EXAMPLE
    .\Sync-ToB2.ps1 -LocalPath "C:\MyFolder" -DeleteRemote -ExcludeExtensions @(".cache") -ExcludeFolders @("*cache*")
.EXAMPLE
    .\Sync-ToB2.ps1 -LocalPath "C:\MyFolder" -EmailTo @("admin@example.com", "backup@example.com") -EmailFrom "backup@example.com"
.EXAMPLE
    .\Sync-ToB2.ps1 -LocalPath "C:\MyFolder" -EmailTo "admin@example.com" -EmailFrom "backup@example.com"
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$ApplicationKeyId = "",
    
    [Parameter(Mandatory=$false)]
    [string]$ApplicationKey = "",
    
    [Parameter(Mandatory=$false)]
    [string]$BucketName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$LocalPath = "",
    
    [Parameter(Mandatory=$false)]
    [string]$RemotePrefix = "",
    
    [Parameter(Mandatory=$false)]
    [string[]]$ExcludeExtensions = @("*.tmp", "*.lnk", "*.pst", "*.ost", "*.msi", "*.exe", "*.dll"),
    
    [Parameter(Mandatory=$false)]
    [string[]]$ExcludeFolders = @("*\$RECYCLE.BIN\*", "*downloads*", "*appdata*", "*backup*", "*archive*", "*cache*"),
    
    [Parameter(Mandatory=$false)]
    [string]$BrevoApiKey = "",
    
    [Parameter(Mandatory=$false)]
    [string[]]$EmailTo = @(""),
    
    [Parameter(Mandatory=$false)]
    [string]$EmailFrom = "",
    
    [Parameter(Mandatory=$false)]
    [string]$EmailFromName = "B2 Backup System",
    
    [Parameter(Mandatory=$false)]
    [long]$LargeFileThreshold = 2GB,
    
    [Parameter(Mandatory=$false)]
    [string]$HashCacheFile = "",
    
    [switch]$DeleteRemote,
    
    [switch]$ShowErrors
)

# Set error action preference based on ShowErrors flag
if ($ShowErrors) {
    $ErrorActionPreference = "Continue"
    $VerbosePreference = "Continue"
} else {
    $ErrorActionPreference = "SilentlyContinue"
    $VerbosePreference = "SilentlyContinue"
}

# Check PowerShell version and offer upgrade
$psVersion = $PSVersionTable.PSVersion
Write-Host "`n=== Backblaze B2 Sync Script ===" -ForegroundColor Yellow
Write-Host "PowerShell Version: $($psVersion.Major).$($psVersion.Minor).$($psVersion.Patch)" -ForegroundColor Cyan

if ($psVersion.Major -lt 7) {
    Write-Host "`n⚠ WARNING: PowerShell 7+ is required for optimal performance" -ForegroundColor Yellow
    Write-Host "Current version: PowerShell $($psVersion.Major).$($psVersion.Minor)" -ForegroundColor Yellow
    Write-Host "PowerShell 7+ provides:" -ForegroundColor White
    Write-Host "  - Parallel processing (up to 8x faster file analysis)" -ForegroundColor White
    Write-Host "  - Improved API compatibility" -ForegroundColor White
    Write-Host "  - Better error handling" -ForegroundColor White
    
    $downloadUrl = "https://aka.ms/powershell-release?tag=stable"
    Write-Host "`nDownloading PowerShell 7 installer..." -ForegroundColor Cyan
    
    try {
        $installerPath = Join-Path $env:TEMP "PowerShell-7-win-x64.msi"
        
        # Download the installer
        Invoke-WebRequest -Uri $downloadUrl -OutFile $installerPath -UseBasicParsing
        
        Write-Host "✓ Installer downloaded to: $installerPath" -ForegroundColor Green
        Write-Host "`nPlease install PowerShell 7 and run this script again using:" -ForegroundColor Yellow
        Write-Host "  pwsh.exe -File `"$($MyInvocation.MyCommand.Path)`"" -ForegroundColor Cyan
        Write-Host "`nOpening installer..." -ForegroundColor Cyan
        
        Start-Process -FilePath $installerPath -Wait
        
        Write-Host "`n✓ Installation complete. Please restart and run with 'pwsh.exe' instead of 'powershell.exe'" -ForegroundColor Green
        exit 0
    }
    catch {
        Write-Host "✗ Failed to download installer: $_" -ForegroundColor Red
        Write-Host "`nPlease manually download PowerShell 7 from:" -ForegroundColor Yellow
        Write-Host "https://github.com/PowerShell/PowerShell/releases/latest" -ForegroundColor Cyan
        exit 1
    }
}

# Function to authorize with B2
function Get-B2Authorization {
    param($KeyId, $Key)
    
    $authString = "${KeyId}:${Key}"
    $authBytes = [System.Text.Encoding]::UTF8.GetBytes($authString)
    $authB64 = [System.Convert]::ToBase64String($authBytes)
    
    $headers = @{
        "Authorization" = "Basic $authB64"
    }
    
    try {
        $response = Invoke-RestMethod -Uri "https://api.backblazeb2.com/b2api/v2/b2_authorize_account" `
            -Method Get -Headers $headers -SkipHeaderValidation
        return $response
    }
    catch {
        Write-Host "✗ Authorization failed" -ForegroundColor Red
        if ($ShowErrors) {
            Write-Error "Authorization failed: $_"
        }
        exit 1
    }
}

# Function to get bucket ID
function Get-B2BucketId {
    param($Auth, $BucketName)
    
    $headers = @{
        "Authorization" = $Auth.authorizationToken
    }
    
    $body = @{
        "accountId" = $Auth.accountId
        "bucketName" = $BucketName
    } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Uri "$($Auth.apiUrl)/b2api/v2/b2_list_buckets" `
            -Method Post -Headers $headers -Body $body -ContentType "application/json" -SkipHeaderValidation
        
        $bucket = $response.buckets | Where-Object { $_.bucketName -eq $BucketName }
        
        if (-not $bucket) {
            Write-Host "✗ Bucket '$BucketName' not found" -ForegroundColor Red
            if ($ShowErrors) {
                Write-Error "Bucket '$BucketName' not found"
            }
            exit 1
        }
        
        return $bucket.bucketId
    }
    catch {
        Write-Host "✗ Failed to get bucket ID" -ForegroundColor Red
        if ($ShowErrors) {
            Write-Error "Failed to get bucket ID: $_"
        }
        exit 1
    }
}

# Function to list all files in B2 bucket
function Get-B2FileList {
    param($Auth, $BucketId, $Prefix)
    
    $headers = @{
        "Authorization" = $Auth.authorizationToken
    }
    
    $allFiles = @{}
    $nextFileName = $null
    
    do {
        $body = @{
            "bucketId" = $BucketId
            "maxFileCount" = 10000
        }
        
        if ($Prefix) {
            $body["prefix"] = $Prefix
        }
        
        if ($nextFileName) {
            $body["startFileName"] = $nextFileName
        }
        
        try {
            $response = Invoke-RestMethod -Uri "$($Auth.apiUrl)/b2api/v2/b2_list_file_names" `
                -Method Post -Headers $headers -Body ($body | ConvertTo-Json) -ContentType "application/json" -SkipHeaderValidation
            
            foreach ($file in $response.files) {
                $allFiles[$file.fileName] = @{
                    fileId = $file.fileId
                    size = $file.contentLength
                    sha1 = $file.contentSha1
                    uploadTimestamp = $file.uploadTimestamp
                }
            }
            
            $nextFileName = $response.nextFileName
        }
        catch {
            Write-Host "✗ Failed to list files" -ForegroundColor Red
            if ($ShowErrors) {
                Write-Error "Failed to list files: $_"
            }
            exit 1
        }
    } while ($nextFileName)
    
    return $allFiles
}

# Function to get upload URL
function Get-B2UploadUrl {
    param($Auth, $BucketId)
    
    $headers = @{
        "Authorization" = $Auth.authorizationToken
    }
    
    $body = @{
        "bucketId" = $BucketId
    } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Uri "$($Auth.apiUrl)/b2api/v2/b2_get_upload_url" `
            -Method Post -Headers $headers -Body $body -ContentType "application/json" -SkipHeaderValidation
        return $response
    }
    catch {
        if ($ShowErrors) {
            Write-Error "Failed to get upload URL: $_"
        }
        return $null
    }
}

# Function to calculate SHA1 hash
function Get-FileSHA1 {
    param($FilePath)
    
    $sha1 = [System.Security.Cryptography.SHA1]::Create()
    $stream = [System.IO.File]::OpenRead($FilePath)
    $hash = $sha1.ComputeHash($stream)
    $stream.Close()
    
    return [System.BitConverter]::ToString($hash).Replace("-", "").ToLower()
}

# Function to load hash cache
function Get-HashCache {
    param($CacheFile)
    
    if ($CacheFile -and (Test-Path $CacheFile)) {
        try {
            $cache = Get-Content $CacheFile -Raw | ConvertFrom-Json
            $hashTable = @{}
            foreach ($prop in $cache.PSObject.Properties) {
                $hashTable[$prop.Name] = $prop.Value
            }
            Write-Host "✓ Loaded hash cache with $($hashTable.Count) entries" -ForegroundColor Green
            return $hashTable
        }
        catch {
            if ($ShowErrors) {
                Write-Warning "Failed to load hash cache: $_"
            }
            return @{}
        }
    }
    return @{}
}

# Function to save hash cache
function Save-HashCache {
    param($CacheFile, $Cache)
    
    if ($CacheFile) {
        try {
            $Cache | ConvertTo-Json | Set-Content $CacheFile
            Write-Host "✓ Saved hash cache with $($Cache.Count) entries" -ForegroundColor Green
        }
        catch {
            if ($ShowErrors) {
                Write-Warning "Failed to save hash cache: $_"
            }
        }
    }
}

# Function to get cached hash or calculate new one
function Get-CachedFileHash {
    param($FilePath, $LastWriteTime, $FileSize, $Cache)
    
    $cacheKey = "$FilePath|$LastWriteTime|$FileSize"
    
    if ($Cache.ContainsKey($cacheKey)) {
        return $Cache[$cacheKey]
    }
    
    $hash = Get-FileSHA1 -FilePath $FilePath
    $Cache[$cacheKey] = $hash
    return $hash
}

# Function to upload a single file
function Upload-B2File {
    param($UploadUrl, $UploadToken, $LocalPath, $RemoteName, $LargeFileThreshold)
    
    $sha1Hash = Get-FileSHA1 -FilePath $LocalPath
    
    # URL encode the filename
    $encodedFileName = [System.Uri]::EscapeDataString($RemoteName)
    
    $headers = @{
        "Authorization" = $UploadToken
        "X-Bz-File-Name" = $encodedFileName
        "Content-Type" = "b2/x-auto"
        "X-Bz-Content-Sha1" = $sha1Hash
    }
    
    try {
        # For files larger than threshold, use streaming upload
        $fileInfo = Get-Item $LocalPath
        if ($fileInfo.Length -gt $LargeFileThreshold) {
            # Use WebRequest for large files with streaming
            $request = [System.Net.HttpWebRequest]::Create($UploadUrl)
            $request.Method = "POST"
            $request.AllowWriteStreamBuffering = $false
            $request.SendChunked = $false
            $request.ContentLength = $fileInfo.Length
            $request.Timeout = 3600000  # 1 hour timeout
            $request.ReadWriteTimeout = 3600000  # 1 hour read/write timeout
            $request.KeepAlive = $true
            
            foreach ($key in $headers.Keys) {
                if ($key -eq "Authorization") {
                    $request.Headers.Add("Authorization", $headers[$key])
                } elseif ($key -eq "Content-Type") {
                    $request.ContentType = $headers[$key]
                } else {
                    $request.Headers.Add($key, $headers[$key])
                }
            }
            
            # Stream the file in chunks
            $bufferSize = 4MB  # Increased buffer size for better performance
            $buffer = New-Object byte[] $bufferSize
            
            $fileStream = $null
            $requestStream = $null
            $response = $null
            
            try {
                $fileStream = [System.IO.File]::OpenRead($LocalPath)
                $requestStream = $request.GetRequestStream()
                
                $totalBytes = $fileInfo.Length
                $bytesWritten = 0
                $bytesRead = 0
                
                while (($bytesRead = $fileStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                    $requestStream.Write($buffer, 0, $bytesRead)
                    $requestStream.Flush()
                    $bytesWritten += $bytesRead
                    
                    # Progress indicator for large files
                    if ($totalBytes -gt 0) {
                        $percentComplete = [math]::Round(($bytesWritten / $totalBytes) * 100, 1)
                        Write-Progress -Activity "Uploading $RemoteName" -Status "$percentComplete% Complete" -PercentComplete $percentComplete
                    }
                }
                
                Write-Progress -Activity "Uploading $RemoteName" -Completed
            }
            finally {
                if ($fileStream -ne $null) {
                    $fileStream.Close()
                    $fileStream.Dispose()
                }
                if ($requestStream -ne $null) {
                    $requestStream.Close()
                    $requestStream.Dispose()
                }
            }
            
            try {
                $response = $request.GetResponse()
                $responseStream = $response.GetResponseStream()
                $reader = New-Object System.IO.StreamReader($responseStream)
                $responseBody = $reader.ReadToEnd()
                $reader.Close()
                $responseStream.Close()
                $response.Close()
                
                return ($responseBody | ConvertFrom-Json)
            }
            catch {
                if ($response -ne $null) {
                    $response.Close()
                }
                throw
            }
        }
        else {
            # For smaller files, use the standard method
            $fileBytes = [System.IO.File]::ReadAllBytes($LocalPath)
            $response = Invoke-RestMethod -Uri $UploadUrl -Method Post `
                -Headers $headers -Body $fileBytes -SkipHeaderValidation
            
            return $response
        }
    }
    catch {
        Write-Error "Failed to upload $RemoteName : $_"
        return $null
    }
}

# Function to delete a file from B2
function Remove-B2File {
    param($Auth, $FileName, $FileId)
    
    $headers = @{
        "Authorization" = $Auth.authorizationToken
    }
    
    $body = @{
        "fileName" = $FileName
        "fileId" = $FileId
    } | ConvertTo-Json
    
    try {
        Invoke-RestMethod -Uri "$($Auth.apiUrl)/b2api/v2/b2_delete_file_version" `
            -Method Post -Headers $headers -Body $body -ContentType "application/json" -SkipHeaderValidation | Out-Null
        return $true
    }
    catch {
        if ($ShowErrors) {
            Write-Error "Failed to delete $FileName : $_"
        }
        return $false
    }
}

# Function to send email via Brevo API
function Send-BrevoEmail {
    param(
        [string]$ApiKey,
        [string[]]$To,
        [string]$From,
        [string]$FromName,
        [string]$Subject,
        [string]$HtmlContent
    )
    
    if (-not $ApiKey -or $To.Count -eq 0 -or -not $From) {
        Write-Host "Email notification skipped - missing required email parameters" -ForegroundColor Yellow
        return $false
    }
    
    $headers = @{
        "accept" = "application/json"
        "api-key" = $ApiKey
        "content-type" = "application/json"
    }
    
    # Build recipient list
    $recipients = @()
    foreach ($email in $To) {
        $recipients += @{
            "email" = $email
        }
    }
    
    $body = @{
        "sender" = @{
            "name" = $FromName
            "email" = $From
        }
        "to" = $recipients
        "subject" = $Subject
        "htmlContent" = $HtmlContent
    } | ConvertTo-Json -Depth 10
    
    try {
        $response = Invoke-RestMethod -Uri "https://api.brevo.com/v3/smtp/email" `
            -Method Post -Headers $headers -Body $body -SkipHeaderValidation
        Write-Host "✓ Email notification sent successfully to $($To.Count) recipient(s)" -ForegroundColor Green
        return $true
    }
    catch {
        if ($ShowErrors) {
            Write-Error "Failed to send email: $_"
        }
        return $false
    }
}

# Main execution
Write-Host ""

# Validate local path
if (-not (Test-Path $LocalPath -PathType Container)) {
    Write-Host "✗ Local path not found or not a directory: $LocalPath" -ForegroundColor Red
    exit 1
}

$LocalPath = (Get-Item $LocalPath).FullName

# Authorize
Write-Host "`nAuthenticating with B2..." -ForegroundColor Cyan
$auth = Get-B2Authorization -KeyId $ApplicationKeyId -Key $ApplicationKey
Write-Host "✓ Authentication successful" -ForegroundColor Green

# Get bucket ID
Write-Host "Finding bucket '$BucketName'..." -ForegroundColor Cyan
$bucketId = Get-B2BucketId -Auth $auth -BucketName $BucketName
Write-Host "✓ Bucket found (ID: $bucketId)" -ForegroundColor Green

# Get remote file list
Write-Host "Retrieving remote file list..." -ForegroundColor Cyan
$remoteFiles = Get-B2FileList -Auth $auth -BucketId $bucketId -Prefix $RemotePrefix
Write-Host "✓ Found $($remoteFiles.Count) remote file(s)" -ForegroundColor Green

# Get local file list
Write-Host "Scanning local files..." -ForegroundColor Cyan
$localFiles = Get-ChildItem -Path $LocalPath -File -Recurse -ErrorAction SilentlyContinue

# Filter out excluded extensions
$originalCount = $localFiles.Count
$excludedByExtension = 0
$excludedByFolder = 0

if ($ExcludeExtensions.Count -gt 0) {
    $localFiles = $localFiles | Where-Object { 
        $ext = $_.Extension.ToLower()
        $ExcludeExtensions -notcontains $ext
    }
    $excludedByExtension = $originalCount - $localFiles.Count
}

# Filter out excluded folders using wildcards
if ($ExcludeFolders.Count -gt 0) {
    $beforeFolderFilter = $localFiles.Count
    $localFiles = $localFiles | Where-Object {
        $filePath = $_.FullName
        $shouldInclude = $true
        
        foreach ($pattern in $ExcludeFolders) {
            # Convert the pattern to work with file paths
            if ($filePath -like $pattern) {
                $shouldInclude = $false
                break
            }
        }
        
        $shouldInclude
    }
    $excludedByFolder = $beforeFolderFilter - $localFiles.Count
}

# Display exclusion summary
$totalExcluded = $excludedByExtension + $excludedByFolder
if ($totalExcluded -gt 0) {
    Write-Host "✓ Found $($localFiles.Count) local file(s) ($excludedByExtension excluded by extension, $excludedByFolder excluded by folder)" -ForegroundColor Green
} else {
    Write-Host "✓ Found $($localFiles.Count) local file(s)" -ForegroundColor Green
}

# Build local file map
$localFileMap = @{}
foreach ($file in $localFiles) {
    $relativePath = $file.FullName.Substring($LocalPath.Length).TrimStart('\', '/').Replace('\', '/')
    
    if ($RemotePrefix) {
        $remotePath = "$RemotePrefix/$relativePath"
    } else {
        $remotePath = $relativePath
    }
    
    $localFileMap[$remotePath] = @{
        localPath = $file.FullName
        size = $file.Length
        lastModified = $file.LastWriteTimeUtc
    }
}

# Determine what needs to be uploaded
Write-Host "`nAnalyzing changes..." -ForegroundColor Yellow

# Load hash cache
$hashCache = Get-HashCache -CacheFile $HashCacheFile

$toUpload = @()
$toSkip = @()

$processedCount = 0
$totalFiles = $localFileMap.Count
$hashCalculated = 0
$hashFromCache = 0

# Convert hashtable to array for parallel processing
$fileArray = @()
foreach ($key in $localFileMap.Keys) {
    $fileArray += [PSCustomObject]@{
        remotePath = $key
        localPath = $localFileMap[$key].localPath
        size = $localFileMap[$key].size
        lastModified = $localFileMap[$key].lastModified
    }
}

# Process files in parallel for faster hash calculation
$results = $fileArray | ForEach-Object -Parallel {
    $fileData = $_
    $remotePath = $fileData.remotePath
    $localPath = $fileData.localPath
    $remoteFiles = $using:remoteFiles
    $hashCache = $using:hashCache
    
    # Recreate the hash function in parallel context
    function Get-FileSHA1Local {
        param($FilePath)
        $sha1 = [System.Security.Cryptography.SHA1]::Create()
        $stream = [System.IO.File]::OpenRead($FilePath)
        $hash = $sha1.ComputeHash($stream)
        $stream.Close()
        return [System.BitConverter]::ToString($hash).Replace("-", "").ToLower()
    }
    
    function Get-CachedFileHashLocal {
        param($FilePath, $LastWriteTime, $FileSize, $Cache)
        $cacheKey = "$FilePath|$LastWriteTime|$FileSize"
        if ($Cache.ContainsKey($cacheKey)) {
            return @{ hash = $Cache[$cacheKey]; fromCache = $true }
        }
        $hash = Get-FileSHA1Local -FilePath $FilePath
        return @{ hash = $hash; fromCache = $false; cacheKey = $cacheKey }
    }
    
    $result = @{
        remotePath = $remotePath
        localPath = $localPath
        action = "skip"
        reason = ""
    }
    
    if ($remoteFiles.ContainsKey($remotePath)) {
        # File exists remotely, check if it needs updating
        $remoteFile = $remoteFiles[$remotePath]
        
        # Get file info
        $fileInfo = Get-Item -LiteralPath $localPath
        
        # Calculate or get cached hash
        $hashResult = Get-CachedFileHashLocal -FilePath $localPath `
            -LastWriteTime $fileInfo.LastWriteTimeUtc.ToString("o") `
            -FileSize $fileInfo.Length `
            -Cache $hashCache
        
        $localHash = $hashResult.hash
        $result.fromCache = $hashResult.fromCache
        $result.cacheKey = $hashResult.cacheKey
        $result.hashValue = $localHash
        
        if ($localHash -ne $remoteFile.sha1) {
            $result.action = "upload"
            $result.reason = "Modified"
        }
    } else {
        # New file - only calculate hash if we're going to upload
        $result.action = "upload"
        $result.reason = "New"
        
        # Pre-calculate hash for new files
        $fileInfo = Get-Item -LiteralPath $localPath
        $hashResult = Get-CachedFileHashLocal -FilePath $localPath `
            -LastWriteTime $fileInfo.LastWriteTimeUtc.ToString("o") `
            -FileSize $fileInfo.Length `
            -Cache $hashCache
        
        $result.fromCache = $hashResult.fromCache
        $result.cacheKey = $hashResult.cacheKey
        $result.hashValue = $hashResult.hash
    }
    
    return $result
} -ThrottleLimit 8

# Process results
foreach ($result in $results) {
    $processedCount++
    
    # Update hash cache with new entries
    if ($result.cacheKey -and $result.hashValue) {
        $hashCache[$result.cacheKey] = $result.hashValue
    }
    
    # Track cache usage
    if ($result.fromCache) {
        $hashFromCache++
    } else {
        $hashCalculated++
    }
    
    # Show progress
    if ($totalFiles -gt 0) {
        $percentComplete = [math]::Round(($processedCount / $totalFiles) * 100, 1)
        Write-Progress -Activity "Analyzing changes" -Status "Processed $processedCount of $totalFiles files ($percentComplete%)" -PercentComplete $percentComplete
    }
    
    if ($result.action -eq "upload") {
        $toUpload += @{
            remotePath = $result.remotePath
            localPath = $result.localPath
            reason = $result.reason
        }
    } else {
        $toSkip += $result.remotePath
    }
}

Write-Progress -Activity "Analyzing changes" -Completed

# Save updated hash cache
if ($HashCacheFile) {
    Save-HashCache -CacheFile $HashCacheFile -Cache $hashCache
}

Write-Host "✓ Analysis complete - Hashes: $hashCalculated calculated, $hashFromCache from cache" -ForegroundColor Green

# Determine what needs to be deleted
$toDelete = @()
if ($DeleteRemote) {
    foreach ($remotePath in $remoteFiles.Keys) {
        if (-not $localFileMap.ContainsKey($remotePath)) {
            $toDelete += @{
                remotePath = $remotePath
                fileId = $remoteFiles[$remotePath].fileId
            }
        }
    }
}

# Display summary
Write-Host "`n--- Sync Summary ---" -ForegroundColor Cyan
Write-Host "Files to upload: $($toUpload.Count)" -ForegroundColor $(if ($toUpload.Count -gt 0) { "Yellow" } else { "Green" })
Write-Host "Files unchanged: $($toSkip.Count)" -ForegroundColor Green
if ($DeleteRemote) {
    Write-Host "Files to delete: $($toDelete.Count)" -ForegroundColor $(if ($toDelete.Count -gt 0) { "Red" } else { "Green" })
}

# Initialize tracking variables
$uploadSuccess = 0
$uploadFail = 0
$uploadedFiles = @()
$failedFiles = @()

# Upload files
if ($toUpload.Count -gt 0) {
    Write-Host "`nUploading files..." -ForegroundColor Yellow
    
    foreach ($file in $toUpload) {
        $fileInfo = Get-Item $file.localPath
        $sizeMB = [math]::Round($fileInfo.Length / 1MB, 2)
        
        Write-Host "[$($file.reason)] Uploading: $($file.remotePath) ($sizeMB MB)" -ForegroundColor Cyan
        
        # Get fresh upload URL
        $uploadInfo = Get-B2UploadUrl -Auth $auth -BucketId $bucketId
        
        if ($uploadInfo) {
            $result = Upload-B2File -UploadUrl $uploadInfo.uploadUrl `
                -UploadToken $uploadInfo.authorizationToken `
                -LocalPath $file.localPath `
                -RemoteName $file.remotePath `
                -LargeFileThreshold $LargeFileThreshold
            
            if ($result) {
                Write-Host "  ✓ Success" -ForegroundColor Green
                $uploadSuccess++
                $uploadedFiles += @{
                    path = $file.remotePath
                    size = $sizeMB
                    reason = $file.reason
                }
            } else {
                Write-Host "  ✗ Failed" -ForegroundColor Red
                $uploadFail++
                $failedFiles += @{
                    path = $file.remotePath
                    size = $sizeMB
                    reason = $file.reason
                }
            }
        } else {
            Write-Host "  ✗ Failed to get upload URL" -ForegroundColor Red
            $uploadFail++
            $failedFiles += @{
                path = $file.remotePath
                size = $sizeMB
                reason = $file.reason
            }
        }
    }
    
    Write-Host "`nUpload Results:" -ForegroundColor Yellow
    Write-Host "  Successful: $uploadSuccess" -ForegroundColor Green
    Write-Host "  Failed: $uploadFail" -ForegroundColor $(if ($uploadFail -gt 0) { "Red" } else { "Green" })
}

# Initialize deletion tracking variables
$deleteSuccess = 0
$deleteFail = 0
$deletedFiles = @()

# Delete remote files
if ($DeleteRemote -and $toDelete.Count -gt 0) {
    Write-Host "`nDeleting remote files..." -ForegroundColor Yellow
    
    foreach ($file in $toDelete) {
        Write-Host "Deleting: $($file.remotePath)" -ForegroundColor Red
        
        if (Remove-B2File -Auth $auth -FileName $file.remotePath -FileId $file.fileId) {
            Write-Host "  ✓ Deleted" -ForegroundColor Green
            $deleteSuccess++
            $deletedFiles += $file.remotePath
        } else {
            $deleteFail++
        }
    }
    
    Write-Host "`nDeletion Results:" -ForegroundColor Yellow
    Write-Host "  Successful: $deleteSuccess" -ForegroundColor Green
    Write-Host "  Failed: $deleteFail" -ForegroundColor $(if ($deleteFail -gt 0) { "Red" } else { "Green" })
}

Write-Host "`n✓ Sync complete!" -ForegroundColor Green

# Send email summary if configured
if ($BrevoApiKey -and $EmailTo.Count -gt 0 -and $EmailFrom) {
    Write-Host "`nSending email summary..." -ForegroundColor Cyan
    
    # Determine overall status
    $status = "Success"
    $statusColor = "#28a745"
    if ($uploadFail -gt 0 -or ($DeleteRemote -and $deleteFail -gt 0)) {
        $status = "Completed with Errors"
        $statusColor = "#ffc107"
    }
    if ($uploadSuccess -eq 0 -and $uploadFail -gt 0) {
        $status = "Failed"
        $statusColor = "#dc3545"
    }
    
    # Build HTML email
    $html = @"
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: $statusColor; color: white; padding: 20px; border-radius: 5px 5px 0 0; }
        .content { background: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-top: none; }
        .summary { background: white; padding: 15px; margin: 15px 0; border-left: 4px solid $statusColor; }
        .stats { display: flex; justify-content: space-around; margin: 20px 0; }
        .stat { text-align: center; padding: 15px; background: white; border-radius: 5px; flex: 1; margin: 0 5px; }
        .stat-value { font-size: 24px; font-weight: bold; color: $statusColor; }
        .stat-label { color: #666; font-size: 12px; text-transform: uppercase; }
        .file-list { background: white; padding: 15px; margin: 10px 0; border-radius: 5px; max-height: 300px; overflow-y: auto; }
        .file-item { padding: 8px; border-bottom: 1px solid #eee; font-size: 13px; }
        .file-item:last-child { border-bottom: none; }
        .success { color: #28a745; }
        .error { color: #dc3545; }
        .icon { display: inline-block; width: 16px; height: 16px; line-height: 16px; text-align: center; font-weight: bold; margin-right: 5px; border-radius: 3px; }
        .icon-success { background: #28a745; color: white; }
        .icon-error { background: #dc3545; color: white; }
        .icon-delete { background: #dc3545; color: white; }
        .footer { text-align: center; padding: 20px; color: #666; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2 style="margin: 0;">B2 Backup Sync Report</h2>
            <p style="margin: 5px 0 0 0;">Status: $status</p>
        </div>
        <div class="content">
            <div class="summary">
                <strong>Sync Details</strong><br>
                <strong>Bucket:</strong> $BucketName<br>
                <strong>Local Path:</strong> $LocalPath<br>
                <strong>Date:</strong> $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")<br>
                <strong>Computer:</strong> $env:COMPUTERNAME
            </div>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-value">$uploadSuccess</div>
                    <div class="stat-label">Uploaded</div>
                </div>
                <div class="stat">
                    <div class="stat-value">$($toSkip.Count)</div>
                    <div class="stat-label">Skipped</div>
                </div>
                <div class="stat">
                    <div class="stat-value" style="color: #dc3545;">$uploadFail</div>
                    <div class="stat-label">Failed</div>
                </div>
            </div>
"@

    if ($uploadedFiles.Count -gt 0) {
        $html += @"
            <h3>Successfully Uploaded Files ($($uploadedFiles.Count))</h3>
            <div class="file-list">
"@
        foreach ($file in $uploadedFiles) {
            $html += "                <div class='file-item'><span class='icon icon-success'>&#10003;</span> $($file.path) ($($file.size) MB) - $($file.reason)</div>`n"
        }
        $html += "            </div>`n"
    }

    if ($failedFiles.Count -gt 0) {
        $html += @"
            <h3>Failed Uploads ($($failedFiles.Count))</h3>
            <div class="file-list">
"@
        foreach ($file in $failedFiles) {
            $html += "                <div class='file-item'><span class='icon icon-error'>&#10005;</span> $($file.path) ($($file.size) MB)</div>`n"
        }
        $html += "            </div>`n"
    }

    if ($DeleteRemote -and $deletedFiles.Count -gt 0) {
        $html += @"
            <h3>Deleted Remote Files ($($deletedFiles.Count))</h3>
            <div class="file-list">
"@
        foreach ($file in $deletedFiles) {
            $html += "                <div class='file-item'><span class='icon icon-delete'>X</span> $file</div>`n"
        }
        $html += "            </div>`n"
    }

    $html += @"
        </div>
        <div class="footer">
            This is an automated message from your B2 Backup System
        </div>
    </div>
</body>
</html>
"@

    $emailSubject = "B2 Backup Report: $status - $BucketName"
    
    Send-BrevoEmail -ApiKey $BrevoApiKey `
        -To $EmailTo `
        -From $EmailFrom `
        -FromName $EmailFromName `
        -Subject $emailSubject `
        -HtmlContent $html
}

Write-Host ""

