<#
.SYNOPSIS
    Emails a report of newly created Active Directory objects via the Brevo API.

.DESCRIPTION
    Designed to run as a scheduled task (for example hourly). Each run queries
    Active Directory for objects whose whenCreated value falls after the previous
    run, builds a single digest email grouped by object type, and sends it via
    the Brevo transactional email API.

    By default only user objects are reported. Use -ObjectTypes to add groups,
    computers, contacts or organisational units. New types can be added by
    extending the $TypeDefinitions table below.

    State (the last successful run time, plus the GUIDs of recently reported
    objects) is kept in a small JSON file so that:
      - nothing is reported twice, and
      - objects that replicate late to the queried DC are still caught, thanks
        to an overlap window (-OverlapMinutes).
    State is only advanced after the email has been sent successfully, so a
    failed send is retried on the next run.

    Compatible with Windows PowerShell 5.1 and PowerShell 7+. Console output is
    plain ASCII.

.PARAMETER ObjectTypes
    Object types to report on. Any of: User, Group, Computer, Contact,
    OrganizationalUnit. Default: User.

.PARAMETER SearchBase
    Optional DN to limit the search to (default: whole domain).

.PARAMETER ExcludeOU
    Optional list of OU/container DNs to ignore (objects inside them, at any
    depth, are skipped). Useful for staging OUs used by automated processes.

.PARAMETER Server
    Domain controller to query. Defaults to the PDC emulator so that every run
    reads from the same DC.

.PARAMETER StatePath
    Path to the JSON state file. Default: ad-new-object-report.state.json next
    to the script.

.PARAMETER InitialLookbackHours
    How far back to look on the very first run (no state file). Default: 24.

.PARAMETER OverlapMinutes
    Each run re-checks this many minutes before the previous run time, to catch
    objects that replicated late. Already reported objects are de-duplicated by
    GUID. Default: 60.

.PARAMETER BrevoApiKey
    Brevo API key in plain text. Prefer -BrevoApiKeyPath for scheduled tasks.

.PARAMETER BrevoApiKeyPath
    Path to a DPAPI-protected key file created with:
        Read-Host -AsSecureString -Prompt 'Brevo API key' | Export-Clixml -Path C:\Scripts\brevo.key.xml
    The file can only be decrypted by the same Windows account on the same
    machine, so create it as the account the scheduled task runs as.

.PARAMETER EmailTo
    One or more recipient addresses.

.PARAMETER EmailFrom
    Sender address (must be a verified sender in Brevo).

.PARAMETER EmailFromName
    Sender display name. Default: AD Change Monitor.

.PARAMETER MaxRowsPerType
    Maximum rows shown per object type in the email body. If exceeded, the
    full list is attached as a CSV file. Default: 50.

.PARAMETER BulkWarningThreshold
    If the total number of new objects in one run reaches this number, a
    warning banner is shown and the subject is flagged. 0 disables. Default: 20.

.PARAMETER SendWhenEmpty
    Send an email even when nothing new was found (acts as a heartbeat so you
    know the job is still running).

.PARAMETER AlwaysAttachCsv
    Always attach the CSV, not only when the email body is truncated.

.PARAMETER PreviewHtmlPath
    Write the HTML report to this file instead of sending an email. The state
    file is not updated.

.PARAMETER PreviewSampleData
    Use built-in sample objects instead of querying AD (no ActiveDirectory
    module needed). The state file is not updated. Combine with
    -PreviewHtmlPath to view the template, or with the email parameters to
    send a test email.

.PARAMETER LogPath
    Optional log file path. Lines are appended.

.EXAMPLE
    .\ad-new-object-report.ps1 -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk

    Reports new users since the last run.

.EXAMPLE
    .\ad-new-object-report.ps1 -ObjectTypes User,Group,Computer -ExcludeOU 'OU=Autopilot,OU=Devices,DC=corp,DC=example,DC=co,DC=uk' -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk

    Reports new users, groups and computers, ignoring an automated staging OU.

.EXAMPLE
    .\ad-new-object-report.ps1 -PreviewSampleData -ObjectTypes User,Group,Computer -PreviewHtmlPath C:\Temp\preview.html

    Renders the email template with sample data without touching AD or Brevo.

.NOTES
    Scheduled task example (run as a service account that can read AD):

    $script = 'C:\Scripts\ad-new-object-report.ps1'
    $arg    = "-NoProfile -ExecutionPolicy Bypass -File `"$script`" -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk -LogPath C:\Scripts\ad-new-object-report.log"
    $action  = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arg
    $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Hours 1)
    Register-ScheduledTask -TaskName 'AD New Object Report' -Action $action -Trigger $trigger -User 'DOMAIN\svc-adreport' -Password '<password>' -RunLevel Limited
#>

[CmdletBinding()]
param(
    [ValidateSet('User', 'Group', 'Computer', 'Contact', 'OrganizationalUnit')]
    [string[]]$ObjectTypes = @('User'),

    [string]$SearchBase = '',

    [string[]]$ExcludeOU = @(),

    [string]$Server = '',

    [string]$StatePath = '',

    [ValidateRange(1, 8760)]
    [int]$InitialLookbackHours = 24,

    [ValidateRange(0, 1440)]
    [int]$OverlapMinutes = 60,

    [string]$BrevoApiKey = '',

    [string]$BrevoApiKeyPath = '',

    [string[]]$EmailTo = @(),

    [string]$EmailFrom = '',

    [string]$EmailFromName = 'AD Change Monitor',

    [ValidateRange(1, 1000)]
    [int]$MaxRowsPerType = 50,

    [ValidateRange(0, 100000)]
    [int]$BulkWarningThreshold = 20,

    [switch]$SendWhenEmpty,

    [switch]$AlwaysAttachCsv,

    [string]$PreviewHtmlPath = '',

    [switch]$PreviewSampleData,

    [string]$LogPath = ''
)

$ErrorActionPreference = 'Stop'
$Invariant = [Globalization.CultureInfo]::InvariantCulture

if (-not $StatePath) {
    $StatePath = Join-Path -Path $PSScriptRoot -ChildPath 'ad-new-object-report.state.json'
}

# ---------------------------------------------------------------------------
# Object type definitions. To report on another type, add an entry here and
# add its key to the ValidateSet on -ObjectTypes.
#   Filter : LDAP filter that identifies the object type
#   Colour : accent colour used in the email
# ---------------------------------------------------------------------------
$TypeDefinitions = [ordered]@{
    User = @{
        Plural   = 'Users'
        Singular = 'user'
        Filter   = '(&(objectCategory=person)(objectClass=user))'
        Colour   = '#2563eb'
    }
    Group = @{
        Plural   = 'Groups'
        Singular = 'group'
        Filter   = '(objectCategory=group)'
        Colour   = '#7c3aed'
    }
    Computer = @{
        Plural   = 'Computers'
        Singular = 'computer'
        Filter   = '(objectCategory=computer)'
        Colour   = '#059669'
    }
    Contact = @{
        Plural   = 'Contacts'
        Singular = 'contact'
        Filter   = '(&(objectCategory=person)(objectClass=contact))'
        Colour   = '#d97706'
    }
    OrganizationalUnit = @{
        Plural   = 'Organisational units'
        Singular = 'organisational unit'
        Filter   = '(objectCategory=organizationalUnit)'
        Colour   = '#475569'
    }
}

# Keep the requested types in the table's display order, without duplicates
$ObjectTypes = @($TypeDefinitions.Keys | Where-Object { $ObjectTypes -contains $_ })

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
function Write-Log {
    param(
        [string]$Message,
        [ValidateSet('INFO', 'OK', 'WARN', 'ERROR')]
        [string]$Level = 'INFO'
    )
    $line = '{0} [{1}] {2}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    $colour = switch ($Level) {
        'OK'    { 'Green' }
        'WARN'  { 'Yellow' }
        'ERROR' { 'Red' }
        default { 'Gray' }
    }
    Write-Host $line -ForegroundColor $colour
    if ($script:LogPath) {
        try { Add-Content -Path $script:LogPath -Value $line -Encoding UTF8 } catch { }
    }
}

function ConvertTo-HtmlText {
    param([object]$Value)
    return [System.Net.WebUtility]::HtmlEncode([string]$Value)
}

function ConvertTo-UtcDateTime {
    # Handles both a DateTime (PowerShell 7 ConvertFrom-Json) and an ISO 8601
    # string (Windows PowerShell 5.1 ConvertFrom-Json).
    param($Value)
    if ($Value -is [datetime]) { return $Value.ToUniversalTime() }
    return [datetime]::Parse([string]$Value, $script:Invariant,
        [Globalization.DateTimeStyles]'AdjustToUniversal, AssumeUniversal')
}

function Get-PropertyValue {
    # Safe property read that copes with attributes that have no value
    param($InputObject, [string]$Name)
    try { return $InputObject.$Name } catch { return $null }
}

function Get-ParentDN {
    # Strips the first RDN, honouring escaped commas (e.g. CN=Smith\, John,OU=...)
    param([string]$DistinguishedName)
    if ($DistinguishedName -match '^(?:[^,\\]|\\.)+,(.+)$') { return $Matches[1] }
    return ''
}

function Get-ReadablePath {
    # corp.example.co.uk/Staff/London/Jane Doe -> Staff / London
    param([string]$CanonicalName)
    if (-not $CanonicalName) { return '' }
    if ($CanonicalName -match '^(.*[^\\])/(?:[^/\\]|\\.)+$') {
        $parent = $Matches[1]
    }
    else {
        $parent = $CanonicalName
    }
    $segments = @($parent -split '(?<!\\)/' | ForEach-Object { $_ -replace '\\/', '/' })
    if ($segments.Count -le 1) { return '(domain root)' }
    return ($segments[1..($segments.Count - 1)] -join ' / ')
}

function Test-Excluded {
    param([string]$DistinguishedName, [string[]]$Exclusions)
    foreach ($ex in $Exclusions) {
        if (-not $ex) { continue }
        if ($DistinguishedName -like "*,$ex") { return $true }
    }
    return $false
}

function New-ReportRow {
    param([string]$Type, $AdObject)

    $created = [datetime](Get-PropertyValue $AdObject 'whenCreated')
    if ($created.Kind -eq [DateTimeKind]::Utc) { $createdLocal = $created.ToLocalTime() }
    else { $createdLocal = $created }

    $dn       = [string](Get-PropertyValue $AdObject 'DistinguishedName')
    $parentDN = Get-ParentDN $dn

    return [pscustomobject]@{
        Type             = $Type
        Name             = [string](Get-PropertyValue $AdObject 'Name')
        AccountName      = [string](Get-PropertyValue $AdObject 'sAMAccountName')
        OuPath           = Get-ReadablePath ([string](Get-PropertyValue $AdObject 'canonicalName'))
        ParentDN         = $parentDN
        Created          = $createdLocal
        CreatedUtc       = $createdLocal.ToUniversalTime()
        Guid             = [string](Get-PropertyValue $AdObject 'ObjectGUID')
        DefaultContainer = ($parentDN -match '^CN=(Users|Computers),DC=')
    }
}

function Read-State {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    try {
        $raw = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
        $reported = @{}
        foreach ($entry in @($raw.ReportedObjects)) {
            if ($entry -and $entry.Guid) {
                $reported[[string]$entry.Guid] = ConvertTo-UtcDateTime $entry.CreatedUtc
            }
        }
        return [pscustomobject]@{
            LastRunUtc = ConvertTo-UtcDateTime $raw.LastRunUtc
            Reported   = $reported
        }
    }
    catch {
        Write-Log "State file could not be read, treating as first run: $($_.Exception.Message)" 'WARN'
        return $null
    }
}

function Save-State {
    param([string]$Path, [datetime]$LastRunUtc, [hashtable]$Reported)
    $list = @(foreach ($key in $Reported.Keys) {
        [pscustomobject]@{ Guid = $key; CreatedUtc = $Reported[$key].ToString('o', $script:Invariant) }
    })
    $json = [pscustomobject]@{
        LastRunUtc      = $LastRunUtc.ToString('o', $script:Invariant)
        ReportedObjects = $list
    } | ConvertTo-Json -Depth 4
    $tmp = "$Path.tmp"
    Set-Content -LiteralPath $tmp -Value $json -Encoding UTF8
    Move-Item -LiteralPath $tmp -Destination $Path -Force
}

function Get-NewADObjects {
    param([string]$Type, [datetime]$SinceUtc)
    $def  = $script:TypeDefinitions[$Type]
    $when = $SinceUtc.ToString('yyyyMMddHHmmss', $script:Invariant) + '.0Z'
    $params = @{
        LDAPFilter = "(&(whenCreated>=$when)$($def.Filter))"
        Properties = @('whenCreated', 'canonicalName', 'sAMAccountName')
        Server     = $script:Server
    }
    if ($script:SearchBase) { $params.SearchBase = $script:SearchBase }
    return @(Get-ADObject @params)
}

function Get-SampleObjects {
    # Sample data for -PreviewSampleData. Shaped like Get-ADObject output.
    param([string[]]$Types)
    $now  = Get-Date
    $dom  = 'DC=corp,DC=example,DC=co,DC=uk'
    $cdom = 'corp.example.co.uk'
    $make = {
        param($Type, $Name, $Sam, $OuDN, $OuCanonical, $MinutesAgo)
        [pscustomobject]@{
            Type              = $Type
            Name              = $Name
            sAMAccountName    = $Sam
            DistinguishedName = "CN=$Name,$OuDN"
            canonicalName     = "$cdom/$OuCanonical/$Name"
            whenCreated       = $now.AddMinutes(-$MinutesAgo)
            ObjectGUID        = [guid]::NewGuid()
        }
    }
    $all = @(
        & $make 'User'     'Sarah Thompson'       'sthompson'    "OU=Finance,OU=Staff,$dom"         'Staff/Finance'      52
        & $make 'User'     "Emily O'Brien"        'eobrien'      "OU=Marketing,OU=Staff,$dom"       'Staff/Marketing'    37
        & $make 'User'     'Test User1'           'testuser1'    "CN=Users,$dom"                    'Users'              12
        & $make 'Group'    'SG-Finance-ReadOnly'  'SG-Finance-ReadOnly' "OU=Security,OU=Groups,$dom" 'Groups/Security' 45
        & $make 'Computer' 'LON-LT-0142'          'LON-LT-0142$' "OU=Laptops,OU=Devices,$dom"       'Devices/Laptops'    30
        & $make 'Computer' 'DESKTOP-7Q3K9PL'      'DESKTOP-7Q3K9PL$' "CN=Computers,$dom"            'Computers'          8
        & $make 'Contact'  'Brightwell Ltd - Accounts' $null     "OU=External,OU=Contacts,$dom"     'Contacts/External'  25
    )
    return @($all | Where-Object { $Types -contains $_.Type })
}

function Get-CountText {
    param([int]$Count, [string]$Type)
    $def = $script:TypeDefinitions[$Type]
    if ($Count -eq 1) { return "1 $($def.Singular)" }
    return "$Count $($def.Plural.ToLower())"
}

function Build-ReportHtml {
    param(
        [object[]]$Rows,
        [string[]]$Types,
        [string]$DomainName,
        [string]$QueryServer,
        [datetime]$PeriodStartUtc,
        [datetime]$PeriodEndUtc,
        [int]$MaxRows,
        [int]$BulkThreshold,
        [bool]$CsvAttached
    )

    $inv       = $script:Invariant
    $periodFmt = 'dd/MM/yyyy HH:mm'
    $rowFmt    = 'dd/MM/yyyy HH:mm:ss'
    $total     = @($Rows).Count
    $font      = "font-family:'Segoe UI',Arial,Helvetica,sans-serif;"
    $periodTxt = '{0} to {1}' -f $PeriodStartUtc.ToLocalTime().ToString($periodFmt, $inv),
                                 $PeriodEndUtc.ToLocalTime().ToString($periodFmt, $inv)
    if ($total -eq 1) { $headline = '1 new object' } else { $headline = "$total new objects" }

    $sb = New-Object System.Text.StringBuilder

    # Document and header
    [void]$sb.AppendLine(@"
<!DOCTYPE html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>New Active Directory objects</title>
</head>
<body style="margin:0;padding:0;background-color:#f1f5f9;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f1f5f9;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="640" cellpadding="0" cellspacing="0" border="0" style="width:640px;max-width:100%;background-color:#ffffff;border:1px solid #e2e8f0;">
<tr><td style="background-color:#0f172a;padding:24px 28px;$font">
<div style="font-size:12px;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;">Active Directory report</div>
<div style="font-size:22px;font-weight:bold;color:#ffffff;padding-top:4px;">$(ConvertTo-HtmlText $headline)</div>
<div style="font-size:14px;color:#cbd5e1;padding-top:4px;">$(ConvertTo-HtmlText $DomainName) &middot; $(ConvertTo-HtmlText $periodTxt)</div>
</td></tr>
"@)

    # Summary tiles, one per requested type
    $tileWidth = [math]::Floor(100 / [math]::Max(1, $Types.Count))
    [void]$sb.AppendLine('<tr><td style="padding:20px 28px 4px 28px;">')
    [void]$sb.AppendLine('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>')
    foreach ($type in $Types) {
        $def   = $script:TypeDefinitions[$type]
        $count = @($Rows | Where-Object { $_.Type -eq $type }).Count
        if ($count -gt 0) { $numColour = $def.Colour } else { $numColour = '#94a3b8' }
        [void]$sb.AppendLine(@"
<td width="$tileWidth%" style="padding:4px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f8fafc;border:1px solid #e2e8f0;border-top:3px solid $($def.Colour);">
<tr><td align="center" style="padding:12px 6px;$font">
<div style="font-size:26px;font-weight:bold;color:$numColour;">$count</div>
<div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;">$(ConvertTo-HtmlText $def.Plural)</div>
</td></tr></table>
</td>
"@)
    }
    [void]$sb.AppendLine('</tr></table></td></tr>')

    # Bulk creation warning
    if ($BulkThreshold -gt 0 -and $total -ge $BulkThreshold) {
        [void]$sb.AppendLine(@"
<tr><td style="padding:12px 28px 0 28px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#fffbeb;border-left:4px solid #f59e0b;">
<tr><td style="padding:12px 16px;$font font-size:14px;color:#78350f;">
<strong>Bulk creation detected.</strong> $total objects were created in this period (threshold: $BulkThreshold). If this was not a planned import or provisioning job, please investigate.
</td></tr></table>
</td></tr>
"@)
    }

    # Empty state
    if ($total -eq 0) {
        [void]$sb.AppendLine(@"
<tr><td style="padding:20px 28px;$font font-size:14px;color:#475569;">
No new objects were created in this period. This email confirms the scheduled check ran successfully.
</td></tr>
"@)
    }

    # One section per type that has results
    foreach ($type in $Types) {
        $def       = $script:TypeDefinitions[$type]
        $typeRows  = @($Rows | Where-Object { $_.Type -eq $type } | Sort-Object Created)
        if ($typeRows.Count -eq 0) { continue }
        $shownRows = @($typeRows | Select-Object -First $MaxRows)

        [void]$sb.AppendLine(@"
<tr><td style="padding:24px 28px 0 28px;$font">
<div style="font-size:16px;font-weight:bold;color:#0f172a;border-left:4px solid $($def.Colour);padding-left:10px;">New $(ConvertTo-HtmlText $def.Plural.ToLower()) ($($typeRows.Count))</div>
</td></tr>
<tr><td style="padding:10px 28px 0 28px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #e2e8f0;border-collapse:collapse;">
<tr style="background-color:#f8fafc;">
<th align="left" width="32%" style="padding:8px 10px;border-bottom:1px solid #e2e8f0;$font font-size:12px;color:#64748b;text-transform:uppercase;">Name</th>
<th align="left" width="44%" style="padding:8px 10px;border-bottom:1px solid #e2e8f0;$font font-size:12px;color:#64748b;text-transform:uppercase;">Location (OU)</th>
<th align="left" width="24%" style="padding:8px 10px;border-bottom:1px solid #e2e8f0;$font font-size:12px;color:#64748b;text-transform:uppercase;">Created</th>
</tr>
"@)
        foreach ($row in $shownRows) {
            $nameHtml = '<div style="font-weight:bold;color:#0f172a;">' + (ConvertTo-HtmlText $row.Name) + '</div>'
            if ($row.AccountName -and $row.AccountName -ne $row.Name) {
                $nameHtml += '<div style="font-size:12px;color:#64748b;">' + (ConvertTo-HtmlText $row.AccountName) + '</div>'
            }
            $locHtml = '<div style="color:#0f172a;">' + (ConvertTo-HtmlText $row.OuPath)
            if ($row.DefaultContainer) {
                $locHtml += ' <span style="display:inline-block;background-color:#fef3c7;color:#92400e;font-size:11px;font-weight:bold;padding:1px 6px;border-radius:3px;">DEFAULT CONTAINER</span>'
            }
            $locHtml += '</div><div style="font-size:11px;color:#94a3b8;word-break:break-all;">' + (ConvertTo-HtmlText $row.ParentDN) + '</div>'
            $createdTxt = $row.Created.ToString($rowFmt, $inv)

            [void]$sb.AppendLine(@"
<tr>
<td valign="top" style="padding:10px;border-bottom:1px solid #f1f5f9;$font font-size:14px;">$nameHtml</td>
<td valign="top" style="padding:10px;border-bottom:1px solid #f1f5f9;$font font-size:14px;">$locHtml</td>
<td valign="top" style="padding:10px;border-bottom:1px solid #f1f5f9;$font font-size:14px;color:#334155;white-space:nowrap;">$createdTxt</td>
</tr>
"@)
        }
        [void]$sb.AppendLine('</table></td></tr>')

        if ($typeRows.Count -gt $shownRows.Count) {
            [void]$sb.AppendLine(@"
<tr><td style="padding:8px 28px 0 28px;$font font-size:13px;color:#64748b;">
Showing $($shownRows.Count) of $($typeRows.Count) $(ConvertTo-HtmlText $def.Plural.ToLower()). The full list is in the attached CSV file.
</td></tr>
"@)
        }
    }

    # Footer
    $typesTxt = ($Types | ForEach-Object { $script:TypeDefinitions[$_].Plural.ToLower() }) -join ', '
    $tzName   = [TimeZoneInfo]::Local.StandardName
    $csvNote  = ''
    if ($CsvAttached) { $csvNote = '<br>A CSV file containing every object in this report is attached.' }
    [void]$sb.AppendLine(@"
<tr><td style="padding:28px 28px 24px 28px;$font font-size:12px;color:#94a3b8;line-height:18px;">
<hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 12px 0;">
Monitoring: $(ConvertTo-HtmlText $typesTxt). Queried $(ConvertTo-HtmlText $QueryServer) from $(ConvertTo-HtmlText $env:COMPUTERNAME).<br>
Times are shown in server local time ($(ConvertTo-HtmlText $tzName)).$csvNote
</td></tr>
</table>
</td></tr>
</table>
</body>
</html>
"@)

    return $sb.ToString()
}

function Get-ReportSubject {
    param([object[]]$Rows, [string[]]$Types, [string]$DomainName, [int]$BulkThreshold)
    $total = @($Rows).Count
    if ($total -eq 0) { return "[AD] No new objects - $DomainName" }
    $parts = @(foreach ($type in $Types) {
        $count = @($Rows | Where-Object { $_.Type -eq $type }).Count
        if ($count -gt 0) { Get-CountText -Count $count -Type $type }
    })
    if ($total -eq 1) { $noun = 'object' } else { $noun = 'objects' }
    $prefix = '[AD]'
    if ($BulkThreshold -gt 0 -and $total -ge $BulkThreshold) { $prefix = '[AD] [BULK]' }
    return "$prefix $total new $noun ($($parts -join ', ')) - $DomainName"
}

function Get-BrevoApiKey {
    if ($script:BrevoApiKey) { return $script:BrevoApiKey }
    if ($script:BrevoApiKeyPath) {
        $secure = Import-Clixml -LiteralPath $script:BrevoApiKeyPath
        if ($secure -is [System.Management.Automation.PSCredential]) { $secure = $secure.Password }
        $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
        try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    return ''
}

function Send-BrevoEmail {
    param(
        [string]$ApiKey,
        [string[]]$To,
        [string]$From,
        [string]$FromName,
        [string]$Subject,
        [string]$HtmlContent,
        [object[]]$Attachments = @()
    )

    # Windows PowerShell 5.1 may not offer TLS 1.2 by default
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

    $payload = [ordered]@{
        sender      = @{ name = $FromName; email = $From }
        to          = @($To | ForEach-Object { @{ email = $_ } })
        subject     = $Subject
        htmlContent = $HtmlContent
    }
    if ($Attachments.Count -gt 0) { $payload.attachment = $Attachments }

    # Send as UTF-8 bytes; 5.1 would otherwise mangle non-ASCII names
    $json  = $payload | ConvertTo-Json -Depth 10
    $bytes = [Text.Encoding]::UTF8.GetBytes($json)

    try {
        $response = Invoke-RestMethod -Uri 'https://api.brevo.com/v3/smtp/email' -Method Post `
            -Headers @{ 'api-key' = $ApiKey; 'accept' = 'application/json' } `
            -ContentType 'application/json; charset=utf-8' -Body $bytes
        Write-Log "Email sent to $($To.Count) recipient(s). Brevo message ID: $($response.messageId)" 'OK'
        return $true
    }
    catch {
        $detail = $_.Exception.Message
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $detail = "$detail $($_.ErrorDetails.Message)" }
        Write-Log "Failed to send email via Brevo: $detail" 'ERROR'
        return $false
    }
}

function New-CsvAttachment {
    param([object[]]$Rows, [string]$FileName)
    $csv = $Rows | Sort-Object Type, Created | Select-Object Type, Name, AccountName, OuPath, ParentDN,
        @{ n = 'Created'; e = { $_.Created.ToString('yyyy-MM-dd HH:mm:ss', $script:Invariant) } },
        Guid | ConvertTo-Csv -NoTypeInformation
    $text  = ($csv -join "`r`n") + "`r`n"
    # BOM so Excel opens accented names correctly
    $bytes = [Text.Encoding]::UTF8.GetPreamble() + [Text.Encoding]::UTF8.GetBytes($text)
    return @{ name = $FileName; content = [Convert]::ToBase64String([byte[]]$bytes) }
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
$runStartUtc = (Get-Date).ToUniversalTime()
$isPreview   = [bool]$PreviewHtmlPath -or $PreviewSampleData

Write-Log '=== New AD object report ==='
Write-Log "Object types: $($ObjectTypes -join ', ')"

# Validate email settings before doing any work
$apiKey = ''
if (-not $PreviewHtmlPath) {
    $EmailTo = @($EmailTo | Where-Object { $_ })
    if ($EmailTo.Count -eq 0 -or -not $EmailFrom) {
        Write-Log 'EmailTo and EmailFrom are required unless -PreviewHtmlPath is used.' 'ERROR'
        exit 1
    }
    try { $apiKey = Get-BrevoApiKey }
    catch {
        Write-Log "Could not read Brevo API key from '$BrevoApiKeyPath': $($_.Exception.Message)" 'ERROR'
        exit 1
    }
    if (-not $apiKey) {
        Write-Log 'No Brevo API key supplied. Use -BrevoApiKey or -BrevoApiKeyPath.' 'ERROR'
        exit 1
    }
}

$rows     = @()
$reported = @{}

if ($PreviewSampleData) {
    Write-Log 'Using sample data (AD will not be queried).' 'WARN'
    $domainName     = 'corp.example.co.uk'
    $Server         = 'DC01.corp.example.co.uk'
    $periodStartUtc = $runStartUtc.AddHours(-1)
    $rows = @(Get-SampleObjects -Types $ObjectTypes | ForEach-Object { New-ReportRow -Type $_.Type -AdObject $_ })
}
else {
    try {
        Import-Module ActiveDirectory -ErrorAction Stop -Verbose:$false
        if (-not $Server) { $Server = (Get-ADDomain).PDCEmulator }
        $domainName = (Get-ADDomain -Server $Server).DNSRoot
    }
    catch {
        Write-Log "Could not load the ActiveDirectory module or contact a DC: $($_.Exception.Message)" 'ERROR'
        exit 1
    }
    Write-Log "Domain: $domainName, querying $Server"

    $state = Read-State -Path $StatePath
    if ($state) {
        $periodStartUtc = $state.LastRunUtc
        $windowStartUtc = $state.LastRunUtc.AddMinutes(-$OverlapMinutes)
        $reported       = $state.Reported
        Write-Log ('Last run: {0:yyyy-MM-dd HH:mm:ss} UTC, searching from {1:yyyy-MM-dd HH:mm:ss} UTC' -f $state.LastRunUtc, $windowStartUtc)
    }
    else {
        $periodStartUtc = $runStartUtc.AddHours(-$InitialLookbackHours)
        $windowStartUtc = $periodStartUtc
        Write-Log "No previous state found, looking back $InitialLookbackHours hour(s)." 'WARN'
    }

    foreach ($type in $ObjectTypes) {
        try {
            $found = Get-NewADObjects -Type $type -SinceUtc $windowStartUtc
        }
        catch {
            Write-Log "AD query for $type objects failed: $($_.Exception.Message)" 'ERROR'
            exit 1
        }
        $new = 0
        foreach ($obj in $found) {
            $row = New-ReportRow -Type $type -AdObject $obj
            if ($reported.ContainsKey($row.Guid)) { continue }
            if (Test-Excluded -DistinguishedName ([string]$obj.DistinguishedName) -Exclusions $ExcludeOU) { continue }
            $rows += $row
            $new++
        }
        Write-Log "$type objects: $new new"
    }
}

$total = $rows.Count
Write-Log "Total new objects: $total"

# State to save after a successful run: drop GUIDs older than the next window
function Save-RunState {
    if ($isPreview) { return }
    $cutoff = $runStartUtc.AddMinutes(-$OverlapMinutes)
    $next   = @{}
    foreach ($key in $reported.Keys) {
        if ($reported[$key] -ge $cutoff) { $next[$key] = $reported[$key] }
    }
    foreach ($row in $rows) {
        if ($row.CreatedUtc -ge $cutoff) { $next[$row.Guid] = $row.CreatedUtc }
    }
    try {
        Save-State -Path $StatePath -LastRunUtc $runStartUtc -Reported $next
    }
    catch {
        Write-Log "Could not write state file '$StatePath': $($_.Exception.Message)" 'ERROR'
        exit 1
    }
}

if ($total -eq 0 -and -not $SendWhenEmpty -and -not $PreviewHtmlPath) {
    Write-Log 'Nothing new to report, no email sent.' 'OK'
    Save-RunState
    exit 0
}

# Truncation and CSV attachment
$truncated = $false
foreach ($type in $ObjectTypes) {
    if (@($rows | Where-Object { $_.Type -eq $type }).Count -gt $MaxRowsPerType) { $truncated = $true }
}
$attachCsv = ($total -gt 0) -and ($truncated -or $AlwaysAttachCsv)

$html = Build-ReportHtml -Rows $rows -Types $ObjectTypes -DomainName $domainName -QueryServer $Server `
    -PeriodStartUtc $periodStartUtc -PeriodEndUtc $runStartUtc -MaxRows $MaxRowsPerType `
    -BulkThreshold $BulkWarningThreshold -CsvAttached $attachCsv
$subject = Get-ReportSubject -Rows $rows -Types $ObjectTypes -DomainName $domainName -BulkThreshold $BulkWarningThreshold

if ($PreviewHtmlPath) {
    [IO.File]::WriteAllText($PreviewHtmlPath, $html, (New-Object Text.UTF8Encoding($false)))
    Write-Log "Subject would be: $subject"
    Write-Log "Preview written to $PreviewHtmlPath (no email sent, state not updated)." 'OK'
    exit 0
}

$attachments = @()
if ($attachCsv) {
    $fileName = 'new-ad-objects-{0}.csv' -f $runStartUtc.ToLocalTime().ToString('yyyyMMdd-HHmm', $Invariant)
    $attachments = @(New-CsvAttachment -Rows $rows -FileName $fileName)
    Write-Log "Attaching $fileName"
}

Write-Log "Sending: $subject"
$sent = Send-BrevoEmail -ApiKey $apiKey -To $EmailTo -From $EmailFrom -FromName $EmailFromName `
    -Subject $subject -HtmlContent $html -Attachments $attachments

if (-not $sent) {
    Write-Log 'State not updated, these objects will be included in the next run.' 'WARN'
    exit 1
}

Save-RunState
Write-Log 'Done.' 'OK'
exit 0
