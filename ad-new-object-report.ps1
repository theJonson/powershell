<#
.SYNOPSIS
    Emails a report of Active Directory changes (objects created, deleted,
    disabled and enabled) via the Brevo API.

.DESCRIPTION
    Designed to run as a scheduled task (for example hourly). Each run checks
    Active Directory for changes since the previous run, builds a single digest
    email grouped by change and object type, and sends it via the Brevo
    transactional email API.

    Change types (-ChangeTypes):
      Created  - objects whose whenCreated is after the previous run (default)
      Deleted  - objects moved to the Deleted Objects container (AD Recycle Bin)
      Disabled - user/computer accounts that have been disabled
      Enabled  - user/computer accounts that were disabled and are now enabled

    Object types (-ObjectTypes): User (default), Group, Computer, Contact,
    OrganizationalUnit. Disabled and Enabled only apply to users and computers.

    State is kept in a small JSON file:
      - the last successful run time,
      - recently reported created/deleted objects (so nothing is reported
        twice, while an overlap window catches objects that replicate late),
      - a snapshot of disabled accounts, compared on each run to detect
        accounts that have been disabled or enabled.
    State is only advanced after the email has been sent successfully, so a
    failed send is retried on the next run.

    Compatible with Windows PowerShell 5.1 and PowerShell 7+. Console output is
    plain ASCII.

.PARAMETER ChangeTypes
    Changes to report. Any of: Created, Deleted, Disabled, Enabled.
    Default: Created. Separate multiple values with commas.
    The first run that includes Disabled or Enabled records a baseline of
    currently disabled accounts and reports those changes from the next run.

.PARAMETER ObjectTypes
    Object types to report on. Any of: User, Group, Computer, Contact,
    OrganizationalUnit. Default: User. Separate multiple types with commas,
    e.g. User,Group,Computer (also works when run via powershell.exe -File).

.PARAMETER SearchBase
    Optional DN to limit the report to (default: whole domain). For deleted
    objects this is matched against the OU they were deleted from.

.PARAMETER ExcludeOU
    Optional list of OU/container DNs to ignore (objects inside them, at any
    depth, are skipped). Useful for staging OUs used by automated processes.
    DNs contain commas, so separate multiple DNs with a semicolon.

.PARAMETER Server
    Domain controller to query. Defaults to the PDC emulator so that every run
    reads from the same DC.

.PARAMETER StatePath
    Path to the JSON state file. Default: ad-new-object-report.state.json next
    to the script.

.PARAMETER InitialLookbackHours
    How far back to look for created/deleted objects on the very first run
    (no state file). Default: 24.

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
    One or more recipient addresses, separated by commas or semicolons. All
    recipients appear on the To line and can see each other.

.PARAMETER EmailFrom
    Sender address (must be a verified sender in Brevo).

.PARAMETER EmailFromName
    Sender display name. Default: AD Change Monitor.

.PARAMETER MaxRowsPerType
    Maximum rows shown per section (change and object type) in the email body.
    If exceeded, the full list is attached as a CSV file. Default: 50.

.PARAMETER BulkWarningThreshold
    If this many objects are created in one run, a warning banner is shown and
    the subject is flagged. 0 disables. Default: 20.

.PARAMETER BulkDeleteThreshold
    If this many objects are deleted in one run, a warning banner is shown and
    the subject is flagged. 0 disables. Default: 5.

.PARAMETER SendWhenEmpty
    Send an email even when nothing was found (acts as a heartbeat so you
    know the job is still running).

.PARAMETER AlwaysAttachCsv
    Always attach the CSV, not only when the email body is truncated.

.PARAMETER PreviewHtmlPath
    Write the HTML report to this file instead of sending an email. The state
    file is not updated.

.PARAMETER PreviewSampleData
    Use built-in sample changes instead of querying AD (no ActiveDirectory
    module needed). The state file is not updated. Combine with
    -PreviewHtmlPath to view the template, or with the email parameters to
    send a test email.

.PARAMETER LogPath
    Optional log file path. Lines are appended.

.PARAMETER CreatorLookup
    How to work out who made each change:
      Auto     - DC Security audit log first; for created objects, fall back
                 to the object owner (default)
      AuditLog - DC Security audit log only
      Owner    - object owner only, created objects only (no auditing needed)
      None     - do not show who made the change
    See NOTES for the audit configuration required.

.PARAMETER DomainControllers
    DCs whose Security logs are searched for audit events. Default: every
    writable DC in the domain. Separate multiple names with commas.

.EXAMPLE
    .\ad-new-object-report.ps1 -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk

    Reports new users since the last run.

.EXAMPLE
    .\ad-new-object-report.ps1 -ChangeTypes Created,Deleted,Disabled,Enabled -ObjectTypes User,Group,Computer -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk

    Reports all four change types for users, groups and computers.

.EXAMPLE
    .\ad-new-object-report.ps1 -PreviewSampleData -ChangeTypes Created,Deleted,Disabled,Enabled -ObjectTypes User,Group,Computer -PreviewHtmlPath C:\Temp\preview.html

    Renders the email template with sample data without touching AD or Brevo.

.NOTES
    WHO MADE THE CHANGE - WHAT NEEDS TO BE CONFIGURED

    The person who made each change is read from the Security event log of
    the DC where the change was made, so every writable DC must log these
    events, and the account running the script must be able to read them
    remotely.

    1. Audit policy (Group Policy linked to the Domain Controllers OU, e.g.
       Default Domain Controllers Policy):
         Computer Configuration > Policies > Windows Settings > Security
         Settings > Advanced Audit Policy Configuration > Audit Policies >
         Account Management:
           - Audit User Account Management       : Success
               4720 created, 4726 deleted, 4725 disabled, 4722 enabled
           - Audit Computer Account Management   : Success
               4741 created, 4743 deleted
           - Audit Security Group Management     : Success
               4727, 4731, 4754 created; 4730, 4734, 4758 deleted
           - Audit Distribution Group Management : Success
               4744, 4749, 4759 created; 4748, 4753, 4763 deleted
         DS Access (contacts and OUs only, or as a catch-all):
           - Audit Directory Service Changes     : Success
               5137 created, 5141 deleted
       And under Security Options:
           - Audit: Force audit policy subcategory settings (Windows Vista or
             later) to override audit policy category settings : Enabled
       Check on each DC with:  auditpol /get /category:"Account Management","DS Access"

    2. SACL for events 5137 and 5141 (contacts and OUs only). These are only
       logged when the parent object has a matching auditing entry.
       In ADSI Edit or ADUC (Advanced Features), on the domain root:
         Properties > Security > Advanced > Auditing > Add
           Principal: Everyone, Type: Success, Applies to: This object and
           all descendant objects, Permissions: Create all child objects,
           Delete, Delete subtree

    3. Security log size. The events must still be in the log when the script
       runs. Set the Security log maximum size on DCs to at least 1 GB
       (Event Log policy or wevtutil sl Security /ms:1073741824) and keep
       the schedule interval short (hourly is ideal).

    4. Remote log access for the script's account:
         - Add the account to the domain's built-in "Event Log Readers" group
           (applies to all DCs).
         - Allow the inbound firewall rule group "Remote Event Log
           Management" on the DCs.
       Test with:  Get-WinEvent -ComputerName DC01 -LogName Security -MaxEvents 1

    If no audit event is found for a created object, Auto falls back to the
    object owner. The owner is the creator when a delegated (non-admin)
    account creates the object, but it is "Domain Admins" when any Domain
    Admin creates it.

    DELETED OBJECTS - WHAT NEEDS TO BE CONFIGURED

    Requires the AD Recycle Bin. By default only administrators can list the
    Deleted Objects container, so grant the script's account read access
    (run as a Domain Admin, replacing the DN and account):

        dsacls "CN=Deleted Objects,DC=corp,DC=example,DC=co,DC=uk" /takeownership
        dsacls "CN=Deleted Objects,DC=corp,DC=example,DC=co,DC=uk" /G "CORP\svc-adreport:LCRP"

    Test as that account:
        Get-ADObject -IncludeDeletedObjects -SearchBase (Get-ADDomain).DeletedObjectsContainer -Filter * -ResultSetSize 5

    Scheduled task example (run as a service account that can read AD):

    $script = 'C:\Scripts\ad-new-object-report.ps1'
    $arg    = "-NoProfile -ExecutionPolicy Bypass -File `"$script`" -BrevoApiKeyPath C:\Scripts\brevo.key.xml -ChangeTypes Created,Deleted,Disabled,Enabled -ObjectTypes User,Group,Computer -EmailTo it@example.co.uk,helpdesk@example.co.uk -EmailFrom alerts@example.co.uk -LogPath C:\Scripts\ad-new-object-report.log"
    $action  = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arg
    $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Hours 1)
    Register-ScheduledTask -TaskName 'AD Change Report' -Action $action -Trigger $trigger -User 'DOMAIN\svc-adreport' -Password '<password>' -RunLevel Limited
#>

[CmdletBinding()]
param(
    # List parameters are validated after splitting, so that -File "a,b" works
    [string[]]$ChangeTypes = @('Created'),

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

    [ValidateRange(0, 100000)]
    [int]$BulkDeleteThreshold = 5,

    [switch]$SendWhenEmpty,

    [switch]$AlwaysAttachCsv,

    [string]$PreviewHtmlPath = '',

    [switch]$PreviewSampleData,

    [string]$LogPath = '',

    [ValidateSet('Auto', 'AuditLog', 'Owner', 'None')]
    [string]$CreatorLookup = 'Auto',

    [string[]]$DomainControllers = @()
)

$ErrorActionPreference = 'Stop'
$Invariant = [Globalization.CultureInfo]::InvariantCulture

if (-not $StatePath) {
    $StatePath = Join-Path -Path $PSScriptRoot -ChildPath 'ad-new-object-report.state.json'
}

# ---------------------------------------------------------------------------
# Object type definitions. To report on another type, add an entry here.
#   Filter        : LDAP filter for live objects
#   DeletedFilter : LDAP filter for deleted objects (objectClass based, as
#                   objectCategory is not reliable on deleted objects)
#   Colour        : accent colour used in the email
# ---------------------------------------------------------------------------
$TypeDefinitions = [ordered]@{
    User = @{
        Plural        = 'Users'
        Singular      = 'user'
        Filter        = '(&(objectCategory=person)(objectClass=user))'
        DeletedFilter = '(&(objectClass=user)(!(objectClass=computer)))'
        Colour        = '#2563eb'
    }
    Group = @{
        Plural        = 'Groups'
        Singular      = 'group'
        Filter        = '(objectCategory=group)'
        DeletedFilter = '(objectClass=group)'
        Colour        = '#7c3aed'
    }
    Computer = @{
        Plural        = 'Computers'
        Singular      = 'computer'
        Filter        = '(objectCategory=computer)'
        DeletedFilter = '(objectClass=computer)'
        Colour        = '#059669'
    }
    Contact = @{
        Plural        = 'Contacts'
        Singular      = 'contact'
        Filter        = '(&(objectCategory=person)(objectClass=contact))'
        DeletedFilter = '(objectClass=contact)'
        Colour        = '#d97706'
    }
    OrganizationalUnit = @{
        Plural        = 'Organisational units'
        Singular      = 'organisational unit'
        Filter        = '(objectCategory=organizationalUnit)'
        DeletedFilter = '(objectClass=organizationalUnit)'
        Colour        = '#475569'
    }
}

# Change type definitions, in display order
$ChangeDefinitions = [ordered]@{
    Created = @{
        Label      = 'Created'
        Heading    = 'New'
        Verb       = 'created'
        ActorLabel = 'Created by'
        WhenLabel  = 'Created'
        Colour     = '#059669'
        Note       = ''
    }
    Deleted = @{
        Label      = 'Deleted'
        Heading    = 'Deleted'
        Verb       = 'deleted'
        ActorLabel = 'Deleted by'
        WhenLabel  = 'Deleted'
        Colour     = '#dc2626'
        Note       = 'Location is where the object was when it was deleted. If this was a mistake, it can be restored from the AD Recycle Bin with Restore-ADObject.'
    }
    Disabled = @{
        Label      = 'Disabled'
        Heading    = 'Disabled'
        Verb       = 'disabled'
        ActorLabel = 'Disabled by'
        WhenLabel  = 'Disabled'
        Colour     = '#64748b'
        Note       = ''
    }
    Enabled = @{
        Label      = 'Enabled'
        Heading    = 'Enabled'
        Verb       = 'enabled'
        ActorLabel = 'Enabled by'
        WhenLabel  = 'Enabled'
        Colour     = '#d97706'
        Note       = 'These accounts were disabled at the previous check and are now enabled. Check that each one was expected, especially older accounts.'
    }
}

# Object types that have an enabled/disabled state
$AccountTypes = @('User', 'Computer')

# Security log event IDs per change and object type. 'Any' events (Directory
# Service Changes) carry the object GUID; the others carry the object SID.
$AuditEvents = @{
    Created  = @{ User = @(4720); Computer = @(4741); Group = @(4727, 4731, 4744, 4749, 4754, 4759); Any = @(5137) }
    Deleted  = @{ User = @(4726); Computer = @(4743); Group = @(4730, 4734, 4748, 4753, 4758, 4763); Any = @(5141) }
    Disabled = @{ User = @(4725); Computer = @(4725) }
    Enabled  = @{ User = @(4722); Computer = @(4722) }
}
$GuidEventIds = @(5137, 5141)

# powershell.exe -File passes "a,b" as one string, so split list parameters here
function Split-ListParameter {
    param([string[]]$Value, [string]$Pattern = '[,;]')
    return @($Value | ForEach-Object { $_ -split $Pattern } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}

function Resolve-ListParameter {
    # Validates values against a definition table and returns them in table order
    param([string[]]$Requested, $Table, [string]$Name)
    $valid   = @($Table.Keys)
    $unknown = @($Requested | Where-Object { $valid -notcontains $_ })
    if ($unknown.Count -gt 0) {
        Write-Host ("Unknown {0}: {1}. Valid values: {2}" -f $Name, ($unknown -join ', '), ($valid -join ', ')) -ForegroundColor Red
        exit 1
    }
    return @($valid | Where-Object { $Requested -contains $_ })
}

$ChangeTypes       = Resolve-ListParameter (Split-ListParameter $ChangeTypes) $ChangeDefinitions 'change type(s)'
$ObjectTypes       = Resolve-ListParameter (Split-ListParameter $ObjectTypes) $TypeDefinitions 'object type(s)'
$EmailTo           = Split-ListParameter $EmailTo
$ExcludeOU         = Split-ListParameter $ExcludeOU ';'
$DomainControllers = Split-ListParameter $DomainControllers

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

function ConvertTo-LocalDateTime {
    param($Value)
    $d = [datetime]$Value
    if ($d.Kind -eq [DateTimeKind]::Utc) { return $d.ToLocalTime() }
    return $d
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

function ConvertTo-ReadablePath {
    # OU=London,OU=Staff,DC=corp,DC=example,DC=co,DC=uk -> Staff / London
    # Also cleans up the parent of an object whose OU was deleted with it.
    param([string]$DistinguishedName)
    if (-not $DistinguishedName) { return '' }
    $parts   = New-Object System.Collections.ArrayList
    $deleted = $false
    foreach ($m in [regex]::Matches($DistinguishedName, '(?:[^,\\]|\\.)+')) {
        $rdn = $m.Value.Trim()
        if ($rdn -match '^DC=') { continue }
        if ($rdn -eq 'CN=Deleted Objects') { $deleted = $true; continue }
        $value = $rdn -replace '^[A-Za-z0-9-]+=', ''
        $value = $value -replace '\\0ADEL:[0-9a-fA-F-]+$', ''
        $value = $value -replace '\\(.)', '$1'
        [void]$parts.Insert(0, $value)
    }
    if ($parts.Count -eq 0) { return '(domain root)' }
    $path = ($parts -join ' / ')
    if ($deleted) { $path += ' (OU also deleted)' }
    return $path
}

function Test-InScope {
    # True if the DN is inside -SearchBase (when set) and not in an excluded OU
    param([string]$DistinguishedName)
    if ($script:SearchBase -and
        $DistinguishedName -ne $script:SearchBase -and
        -not ($DistinguishedName -like "*,$($script:SearchBase)")) { return $false }
    foreach ($ex in $script:ExcludeOU) {
        if ($DistinguishedName -like "*,$ex") { return $false }
    }
    return $true
}

function New-ChangeRow {
    param(
        [string]$Change,
        [string]$Type,
        [string]$Name,
        [string]$AccountName,
        [string]$ParentDN,
        $When,
        [string]$Guid,
        [string]$Sid = '',
        [string]$Owner = '',
        $AccountCreated = $null
    )
    $whenLocal = ConvertTo-LocalDateTime $When
    $created   = $null
    if ($AccountCreated) { $created = ConvertTo-LocalDateTime $AccountCreated }
    return [pscustomobject]@{
        Change           = $Change
        Type             = $Type
        Name             = $Name
        AccountName      = $AccountName
        OuPath           = ConvertTo-ReadablePath $ParentDN
        ParentDN         = $ParentDN
        When             = $whenLocal
        WhenUtc          = $whenLocal.ToUniversalTime()
        Guid             = $Guid
        Sid              = $Sid
        Owner            = $Owner
        DefaultContainer = ($Change -eq 'Created' -and $ParentDN -match '^CN=(Users|Computers),DC=')
        AccountCreated   = $created
        Actor            = ''
        ActorSource      = ''
    }
}

function ConvertFrom-LiveObject {
    # Builds a change row from a live (not deleted) AD object
    param([string]$Change, [string]$Type, $AdObject, $When, $AccountCreated = $null)
    $owner = ''
    $sd = Get-PropertyValue $AdObject 'nTSecurityDescriptor'
    if ($sd) { $owner = [string]$sd.Owner }
    return New-ChangeRow -Change $Change -Type $Type `
        -Name ([string](Get-PropertyValue $AdObject 'Name')) `
        -AccountName ([string](Get-PropertyValue $AdObject 'sAMAccountName')) `
        -ParentDN (Get-ParentDN ([string]$AdObject.DistinguishedName)) `
        -When $When -Guid ([string]$AdObject.ObjectGUID) `
        -Sid ([string](Get-PropertyValue $AdObject 'objectSid')) `
        -Owner $owner -AccountCreated $AccountCreated
}

function Read-State {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    try {
        $raw = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json

        # Reported objects. Older state files stored {Guid, CreatedUtc}.
        $reported = @{}
        foreach ($entry in @($raw.ReportedObjects)) {
            if (-not $entry) { continue }
            if ($entry.PSObject.Properties['Key']) {
                $reported[[string]$entry.Key] = ConvertTo-UtcDateTime $entry.TimeUtc
            }
            elseif ($entry.PSObject.Properties['Guid']) {
                $reported["Created|$($entry.Guid)"] = ConvertTo-UtcDateTime $entry.CreatedUtc
            }
        }

        # Disabled account snapshot: object type -> set of GUIDs
        $snapshot = @{}
        if ($raw.PSObject.Properties['DisabledSnapshot'] -and $raw.DisabledSnapshot) {
            foreach ($p in $raw.DisabledSnapshot.PSObject.Properties) {
                $set = @{}
                foreach ($g in @($p.Value)) { if ($g) { $set[[string]$g] = $true } }
                $snapshot[$p.Name] = $set
            }
        }

        return [pscustomobject]@{
            LastRunUtc = ConvertTo-UtcDateTime $raw.LastRunUtc
            Reported   = $reported
            Snapshot   = $snapshot
        }
    }
    catch {
        Write-Log "State file could not be read, treating as first run: $($_.Exception.Message)" 'WARN'
        return $null
    }
}

function Save-State {
    param([string]$Path, [datetime]$LastRunUtc, [hashtable]$Reported, [hashtable]$Snapshot)
    $list = @(foreach ($key in $Reported.Keys) {
        [pscustomobject]@{ Key = $key; TimeUtc = $Reported[$key].ToString('o', $script:Invariant) }
    })
    $snap = [ordered]@{}
    foreach ($type in $Snapshot.Keys) { $snap[$type] = @($Snapshot[$type].Keys) }
    $json = [pscustomobject]@{
        LastRunUtc       = $LastRunUtc.ToString('o', $script:Invariant)
        ReportedObjects  = $list
        DisabledSnapshot = [pscustomobject]$snap
    } | ConvertTo-Json -Depth 5
    $tmp = "$Path.tmp"
    Set-Content -LiteralPath $tmp -Value $json -Encoding UTF8
    Move-Item -LiteralPath $tmp -Destination $Path -Force
}

function ConvertTo-LdapTime {
    param([datetime]$Utc)
    return $Utc.ToString('yyyyMMddHHmmss', $script:Invariant) + '.0Z'
}

function Get-CreatedADObjects {
    param([string]$Type, [datetime]$SinceUtc)
    $def = $script:TypeDefinitions[$Type]
    $params = @{
        LDAPFilter = "(&(whenCreated>=$(ConvertTo-LdapTime $SinceUtc))$($def.Filter))"
        Properties = @('whenCreated', 'sAMAccountName', 'objectSid')
        Server     = $script:Server
    }
    if ($script:CreatorLookup -eq 'Auto' -or $script:CreatorLookup -eq 'Owner') {
        $params.Properties += 'nTSecurityDescriptor'
    }
    if ($script:SearchBase) { $params.SearchBase = $script:SearchBase }
    return @(Get-ADObject @params)
}

function Get-DeletedADObjects {
    # Deleted (not yet recycled) objects changed since the given time. With the
    # AD Recycle Bin, whenChanged on a deleted object is its deletion time.
    param([string]$Type, [datetime]$SinceUtc)
    $def = $script:TypeDefinitions[$Type]
    $params = @{
        LDAPFilter            = "(&(isDeleted=TRUE)(!(isRecycled=TRUE))(whenChanged>=$(ConvertTo-LdapTime $SinceUtc))$($def.DeletedFilter))"
        SearchBase            = $script:DeletedObjectsDN
        IncludeDeletedObjects = $true
        Properties            = @('whenChanged', 'lastKnownParent', 'msDS-LastKnownRDN', 'sAMAccountName', 'objectSid')
        Server                = $script:Server
    }
    return @(Get-ADObject @params)
}

function Get-DisabledADObjects {
    param([string]$Type)
    $def = $script:TypeDefinitions[$Type]
    $params = @{
        LDAPFilter = "(&$($def.Filter)(userAccountControl:1.2.840.113556.1.4.803:=2))"
        Properties = @('whenCreated', 'whenChanged', 'sAMAccountName', 'objectSid')
        Server     = $script:Server
    }
    if ($script:SearchBase) { $params.SearchBase = $script:SearchBase }
    return @(Get-ADObject @params)
}

function Get-AttributeChangeTime {
    # When an attribute last changed (from replication metadata), or $null
    param([string]$DistinguishedName, [string]$Attribute)
    try {
        $meta = Get-ADReplicationAttributeMetadata -Object $DistinguishedName -Server $script:Server -Properties $Attribute -ErrorAction Stop |
            Where-Object { $_.AttributeName -eq $Attribute } | Select-Object -First 1
        if ($meta -and $meta.LastOriginatingChangeTime) { return [datetime]$meta.LastOriginatingChangeTime }
    }
    catch { }
    return $null
}

function Get-AuditEvents {
    # Reads the relevant events from each DC's Security log in one query per
    # DC. Returns a lookup keyed by "Change|SID" or "Change|GUID", holding the
    # most recent matching event.
    param([string[]]$Computers, [string[]]$Changes, [string[]]$Types, [datetime]$SinceUtc)

    $idToChange = @{}
    foreach ($change in $Changes) {
        $map = $script:AuditEvents[$change]
        if (-not $map) { continue }
        $keys = @($Types) + 'Any'
        foreach ($k in $keys) {
            if ($map.ContainsKey($k)) {
                foreach ($id in $map[$k]) { $idToChange[[int]$id] = $change }
            }
        }
    }
    $ids = @($idToChange.Keys | Sort-Object)
    $lookup = @{}
    $failed = @()
    if ($ids.Count -eq 0) { return [pscustomobject]@{ Lookup = $lookup; FailedDCs = $failed } }

    # Small margin for clock differences between DCs
    $start = $SinceUtc.AddMinutes(-5).ToLocalTime()

    foreach ($dc in $Computers) {
        $shortName = ($dc -split '\.')[0]
        try {
            $events = @(Get-WinEvent -ComputerName $dc -ErrorAction Stop -FilterHashtable @{
                LogName = 'Security'; Id = [int[]]$ids; StartTime = $start
            })
        }
        catch {
            if ($_.FullyQualifiedErrorId -like 'NoMatchingEventsFound*') {
                $events = @()
            }
            else {
                Write-Log "Could not read the Security log on ${dc}: $($_.Exception.Message)" 'WARN'
                $failed += $shortName
                continue
            }
        }
        Write-Log "Audit log on ${dc}: $($events.Count) matching event(s)"

        # Get-WinEvent returns newest first, so the first event per key wins
        foreach ($ev in $events) {
            $change = $idToChange[[int]$ev.Id]
            $data = @{}
            foreach ($d in ([xml]$ev.ToXml()).Event.EventData.Data) { $data[[string]$d.Name] = [string]$d.'#text' }
            if ($script:GuidEventIds -contains $ev.Id) {
                if (-not $data['ObjectGUID']) { continue }
                $key = "$change|" + $data['ObjectGUID'].Trim('{', '}')
            }
            else {
                if (-not $data['TargetSid']) { continue }
                $key = "$change|" + $data['TargetSid']
            }
            if ($lookup.ContainsKey($key)) { continue }
            $lookup[$key] = [pscustomobject]@{
                Actor  = '{0}\{1}' -f $data['SubjectDomainName'], $data['SubjectUserName']
                Source = "audit log on $shortName"
            }
        }
    }

    return [pscustomobject]@{ Lookup = $lookup; FailedDCs = $failed }
}

function Set-RowActors {
    param([object[]]$Rows, $Audit, [string]$Mode)
    foreach ($row in $Rows) {
        $hit = $null
        if ($Audit) {
            $sidKey  = "$($row.Change)|$($row.Sid)"
            $guidKey = "$($row.Change)|$($row.Guid)"
            if ($row.Sid -and $Audit.Lookup.ContainsKey($sidKey)) { $hit = $Audit.Lookup[$sidKey] }
            elseif ($Audit.Lookup.ContainsKey($guidKey)) { $hit = $Audit.Lookup[$guidKey] }
        }
        if ($hit) {
            $row.Actor       = $hit.Actor
            $row.ActorSource = $hit.Source
        }
        elseif ($row.Change -eq 'Created' -and ($Mode -eq 'Auto' -or $Mode -eq 'Owner') -and $row.Owner) {
            $row.Actor = $row.Owner
            if ($row.Owner -match '\\(Domain Admins|Enterprise Admins|Administrators)$') {
                $row.ActorSource = 'object owner (any member of this group)'
            }
            else {
                $row.ActorSource = 'object owner'
            }
        }
        else {
            $row.Actor       = 'Unknown'
            $row.ActorSource = 'no audit event found'
        }
    }
}

function Get-SampleRows {
    # Sample data for -PreviewSampleData
    param([string[]]$Changes, [string[]]$Types)
    $now = Get-Date
    $dom = 'DC=corp,DC=example,DC=co,DC=uk'
    $make = {
        param($Change, $Type, $Name, $Sam, $OuDN, $MinutesAgo, $By, $BySource, $AccountCreated)
        $row = New-ChangeRow -Change $Change -Type $Type -Name $Name -AccountName $Sam -ParentDN $OuDN `
            -When $now.AddMinutes(-$MinutesAgo) -Guid ([string][guid]::NewGuid()) -AccountCreated $AccountCreated
        $row.Actor       = $By
        $row.ActorSource = $BySource
        $row
    }
    $old = Get-Date -Year 2021 -Month 3 -Day 14 -Hour 10 -Minute 12 -Second 0
    $all = @(
        & $make 'Created'  'User'     'Sarah Thompson'      'sthompson'        "OU=Finance,OU=Staff,$dom"     52 'CORP\jbaker'          'audit log on DC01' $null
        & $make 'Created'  'User'     "Emily O'Brien"       'eobrien'          "OU=Marketing,OU=Staff,$dom"   37 'CORP\svc-hrprovision' 'audit log on DC02' $null
        & $make 'Created'  'User'     'Test User1'          'testuser1'        "CN=Users,$dom"                12 'CORP\Domain Admins'   'object owner (any member of this group)' $null
        & $make 'Created'  'Group'    'SG-Finance-ReadOnly' 'SG-Finance-ReadOnly' "OU=Security,OU=Groups,$dom" 45 'CORP\jbaker'        'audit log on DC01' $null
        & $make 'Created'  'Computer' 'LON-LT-0142'         'LON-LT-0142$'     "OU=Laptops,OU=Devices,$dom"   30 'CORP\mpatel'          'audit log on DC02' $null
        & $make 'Created'  'Computer' 'DESKTOP-7Q3K9PL'     'DESKTOP-7Q3K9PL$' "CN=Computers,$dom"             8 'Unknown'              'no audit event found' $null
        & $make 'Created'  'Contact'  'Brightwell Ltd - Accounts' ''           "OU=External,OU=Contacts,$dom" 25 'CORP\akhan'           'audit log on DC01' $null
        & $make 'Deleted'  'User'     'Tom Hughes'          'thughes'          "OU=Leavers,$dom"              41 'CORP\jbaker'          'audit log on DC01' $null
        & $make 'Deleted'  'Group'    'SG-Project-Atlas'    'SG-Project-Atlas' "OU=Security,OU=Groups,$dom"   20 'CORP\mpatel'          'audit log on DC02' $null
        & $make 'Deleted'  'Computer' 'LON-DT-0031'         'LON-DT-0031$'     "OU=Desktops,OU=Devices,$dom"  15 'Unknown'              'no audit event found' $null
        & $make 'Disabled' 'User'     'Priya Shah'          'pshah'            "OU=Sales,OU=Staff,$dom"       33 'CORP\svc-hrprovision' 'audit log on DC02' $null
        & $make 'Disabled' 'Computer' 'LON-LT-0098'         'LON-LT-0098$'     "OU=Laptops,OU=Devices,$dom"   27 'CORP\mpatel'          'audit log on DC01' $null
        & $make 'Enabled'  'User'     'Daniel Ross'         'dross'            "OU=Leavers,$dom"               5 'CORP\tempadmin'       'audit log on DC02' $old
    )
    return @($all | Where-Object { $Changes -contains $_.Change -and $Types -contains $_.Type })
}

function Get-RowCount {
    param([object[]]$Rows, [string]$Change = '', [string]$Type = '')
    return @($Rows | Where-Object {
        (-not $Change -or $_.Change -eq $Change) -and (-not $Type -or $_.Type -eq $Type)
    }).Count
}

function Build-ReportHtml {
    param(
        [object[]]$Rows,
        [string[]]$Changes,
        [string[]]$Types,
        [string]$DomainName,
        [string]$QueryServer,
        [datetime]$PeriodStartUtc,
        [datetime]$PeriodEndUtc,
        [int]$MaxRows,
        [int]$BulkCreateThreshold,
        [int]$BulkDeleteThreshold,
        [bool]$CsvAttached,
        [bool]$ShowActor,
        [string]$AuditNote = ''
    )

    $inv       = $script:Invariant
    $periodFmt = 'dd/MM/yyyy HH:mm'
    $total     = @($Rows).Count
    $font      = "font-family:'Segoe UI',Arial,Helvetica,sans-serif;"
    $createdOnly = ($Changes.Count -eq 1 -and $Changes[0] -eq 'Created')
    if ($ShowActor) { $w = @(25, 35, 22, 18) } else { $w = @(32, 44, 0, 24) }
    $periodTxt = '{0} to {1}' -f $PeriodStartUtc.ToLocalTime().ToString($periodFmt, $inv),
                                 $PeriodEndUtc.ToLocalTime().ToString($periodFmt, $inv)
    if ($createdOnly) {
        if ($total -eq 1) { $headline = '1 new object' } else { $headline = "$total new objects" }
    }
    else {
        if ($total -eq 1) { $headline = '1 change' } else { $headline = "$total changes" }
    }

    $sb = New-Object System.Text.StringBuilder

    # Document and header
    [void]$sb.AppendLine(@"
<!DOCTYPE html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Active Directory changes</title>
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

    # Summary tiles: per object type for a single change type, otherwise per change type
    $tiles = @()
    if ($Changes.Count -eq 1) {
        foreach ($type in $Types) {
            $def = $script:TypeDefinitions[$type]
            $tiles += [pscustomobject]@{ Label = $def.Plural; Colour = $def.Colour; Total = (Get-RowCount $Rows -Type $type) }
        }
    }
    else {
        foreach ($change in $Changes) {
            $def = $script:ChangeDefinitions[$change]
            $tiles += [pscustomobject]@{ Label = $def.Label; Colour = $def.Colour; Total = (Get-RowCount $Rows -Change $change) }
        }
    }
    $tileWidth = [math]::Floor(100 / [math]::Max(1, $tiles.Count))
    [void]$sb.AppendLine('<tr><td style="padding:20px 28px 4px 28px;">')
    [void]$sb.AppendLine('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>')
    foreach ($tile in $tiles) {
        if ($tile.Total -gt 0) { $numColour = $tile.Colour } else { $numColour = '#94a3b8' }
        [void]$sb.AppendLine(@"
<td width="$tileWidth%" style="padding:4px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f8fafc;border:1px solid #e2e8f0;border-top:3px solid $($tile.Colour);">
<tr><td align="center" style="padding:12px 6px;$font">
<div style="font-size:26px;font-weight:bold;color:$numColour;">$($tile.Total)</div>
<div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;">$(ConvertTo-HtmlText $tile.Label)</div>
</td></tr></table>
</td>
"@)
    }
    [void]$sb.AppendLine('</tr></table></td></tr>')

    # Bulk warnings
    $createdCount = Get-RowCount $Rows -Change 'Created'
    $deletedCount = Get-RowCount $Rows -Change 'Deleted'
    if ($BulkCreateThreshold -gt 0 -and $createdCount -ge $BulkCreateThreshold) {
        [void]$sb.AppendLine(@"
<tr><td style="padding:12px 28px 0 28px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#fffbeb;border-left:4px solid #f59e0b;">
<tr><td style="padding:12px 16px;$font font-size:14px;color:#78350f;">
<strong>Bulk creation detected.</strong> $createdCount objects were created in this period (threshold: $BulkCreateThreshold). If this was not a planned import or provisioning job, please investigate.
</td></tr></table>
</td></tr>
"@)
    }
    if ($BulkDeleteThreshold -gt 0 -and $deletedCount -ge $BulkDeleteThreshold) {
        [void]$sb.AppendLine(@"
<tr><td style="padding:12px 28px 0 28px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#fef2f2;border-left:4px solid #dc2626;">
<tr><td style="padding:12px 16px;$font font-size:14px;color:#7f1d1d;">
<strong>Bulk deletion detected.</strong> $deletedCount objects were deleted in this period (threshold: $BulkDeleteThreshold). If this was not planned, investigate now. Deleted objects can be restored from the AD Recycle Bin.
</td></tr></table>
</td></tr>
"@)
    }

    # Empty state
    if ($total -eq 0) {
        if ($createdOnly) { $emptyText = 'No new objects were created in this period.' }
        else { $emptyText = 'No changes were found in this period.' }
        [void]$sb.AppendLine(@"
<tr><td style="padding:20px 28px;$font font-size:14px;color:#475569;">
$emptyText This email confirms the scheduled check ran successfully.
</td></tr>
"@)
    }

    # One section per change type and object type that has results
    $thStyle   = "padding:8px 10px;border-bottom:1px solid #e2e8f0;$font font-size:12px;color:#64748b;text-transform:uppercase;"
    $cellStyle = "padding:10px;border-bottom:1px solid #f1f5f9;$font font-size:14px;"
    foreach ($change in $Changes) {
        $cdef      = $script:ChangeDefinitions[$change]
        $noteShown = $false
        foreach ($type in $Types) {
            $tdef        = $script:TypeDefinitions[$type]
            $sectionRows = @($Rows | Where-Object { $_.Change -eq $change -and $_.Type -eq $type } | Sort-Object When)
            if ($sectionRows.Count -eq 0) { continue }
            $shownRows = @($sectionRows | Select-Object -First $MaxRows)
            if ($Changes.Count -eq 1) { $accent = $tdef.Colour } else { $accent = $cdef.Colour }
            $sectionName = "$($cdef.Heading) $($tdef.Plural.ToLower())"

            [void]$sb.AppendLine(@"
<tr><td style="padding:24px 28px 0 28px;$font">
<div style="font-size:16px;font-weight:bold;color:#0f172a;border-left:4px solid $accent;padding-left:10px;">$(ConvertTo-HtmlText $sectionName) ($($sectionRows.Count))</div>
</td></tr>
"@)
            if ($cdef.Note -and -not $noteShown) {
                $noteShown = $true
                [void]$sb.AppendLine("<tr><td style=`"padding:6px 28px 0 28px;$font font-size:13px;color:#64748b;`">$(ConvertTo-HtmlText $cdef.Note)</td></tr>")
            }
            [void]$sb.AppendLine(@"
<tr><td style="padding:10px 28px 0 28px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #e2e8f0;border-collapse:collapse;">
<tr style="background-color:#f8fafc;">
<th align="left" width="$($w[0])%" style="$thStyle">Name</th>
<th align="left" width="$($w[1])%" style="$thStyle">Location (OU)</th>
"@)
            if ($ShowActor) {
                [void]$sb.AppendLine("<th align=`"left`" width=`"$($w[2])%`" style=`"$thStyle`">$(ConvertTo-HtmlText $cdef.ActorLabel)</th>")
            }
            [void]$sb.AppendLine("<th align=`"left`" width=`"$($w[3])%`" style=`"$thStyle`">$(ConvertTo-HtmlText $cdef.WhenLabel)</th>")
            [void]$sb.AppendLine('</tr>')

            foreach ($row in $shownRows) {
                $nameHtml = '<div style="font-weight:bold;color:#0f172a;">' + (ConvertTo-HtmlText $row.Name) + '</div>'
                if ($row.AccountName -and $row.AccountName -ne $row.Name) {
                    $nameHtml += '<div style="font-size:12px;color:#64748b;">' + (ConvertTo-HtmlText $row.AccountName) + '</div>'
                }
                if ($row.AccountCreated) {
                    $nameHtml += '<div style="font-size:11px;color:#94a3b8;">account created ' + $row.AccountCreated.ToString('dd/MM/yyyy', $inv) + '</div>'
                }
                $locHtml = '<div style="color:#0f172a;">' + (ConvertTo-HtmlText $row.OuPath)
                if ($row.DefaultContainer) {
                    $locHtml += ' <span style="display:inline-block;background-color:#fef3c7;color:#92400e;font-size:11px;font-weight:bold;padding:1px 6px;border-radius:3px;">DEFAULT CONTAINER</span>'
                }
                $locHtml += '</div><div style="font-size:11px;color:#94a3b8;word-break:break-all;">' + (ConvertTo-HtmlText $row.ParentDN) + '</div>'
                $whenHtml = '<div>' + $row.When.ToString('dd/MM/yyyy', $inv) + '</div><div style="font-size:12px;color:#64748b;">' + $row.When.ToString('HH:mm:ss', $inv) + '</div>'

                [void]$sb.AppendLine('<tr>')
                [void]$sb.AppendLine("<td valign=`"top`" style=`"$cellStyle`">$nameHtml</td>")
                [void]$sb.AppendLine("<td valign=`"top`" style=`"$cellStyle`">$locHtml</td>")
                if ($ShowActor) {
                    if ($row.Actor -eq 'Unknown') { $byColour = '#94a3b8' } else { $byColour = '#0f172a' }
                    $byHtml = '<div style="color:' + $byColour + ';">' + (ConvertTo-HtmlText $row.Actor) + '</div>'
                    if ($row.ActorSource) {
                        $byHtml += '<div style="font-size:11px;color:#94a3b8;">' + (ConvertTo-HtmlText $row.ActorSource) + '</div>'
                    }
                    [void]$sb.AppendLine("<td valign=`"top`" style=`"$cellStyle`">$byHtml</td>")
                }
                [void]$sb.AppendLine("<td valign=`"top`" style=`"$cellStyle color:#334155;white-space:nowrap;`">$whenHtml</td>")
                [void]$sb.AppendLine('</tr>')
            }
            [void]$sb.AppendLine('</table></td></tr>')

            if ($sectionRows.Count -gt $shownRows.Count) {
                [void]$sb.AppendLine(@"
<tr><td style="padding:8px 28px 0 28px;$font font-size:13px;color:#64748b;">
Showing $($shownRows.Count) of $($sectionRows.Count) $(ConvertTo-HtmlText $sectionName.ToLower()). The full list is in the attached CSV file.
</td></tr>
"@)
            }
        }
    }

    # Footer
    $typesTxt   = ($Types | ForEach-Object { $script:TypeDefinitions[$_].Plural.ToLower() }) -join ', '
    $changesTxt = ($Changes | ForEach-Object { $script:ChangeDefinitions[$_].Verb }) -join ', '
    $tzName     = [TimeZoneInfo]::Local.StandardName
    $notes      = ''
    if ($CsvAttached) { $notes += '<br>A CSV file containing every change in this report is attached.' }
    if ($AuditNote) { $notes += '<br>' + (ConvertTo-HtmlText $AuditNote) }
    [void]$sb.AppendLine(@"
<tr><td style="padding:28px 28px 24px 28px;$font font-size:12px;color:#94a3b8;line-height:18px;">
<hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 12px 0;">
Monitoring: $(ConvertTo-HtmlText $typesTxt) ($(ConvertTo-HtmlText $changesTxt)). Queried $(ConvertTo-HtmlText $QueryServer) from $(ConvertTo-HtmlText $env:COMPUTERNAME).<br>
Times are shown in server local time ($(ConvertTo-HtmlText $tzName)).$notes
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
    param([object[]]$Rows, [string[]]$Changes, [string[]]$Types, [string]$DomainName, [int]$BulkCreateThreshold, [int]$BulkDeleteThreshold)
    $total       = @($Rows).Count
    $createdOnly = ($Changes.Count -eq 1 -and $Changes[0] -eq 'Created')
    if ($total -eq 0) {
        if ($createdOnly) { return "[AD] No new objects - $DomainName" }
        return "[AD] No changes - $DomainName"
    }

    $prefix = '[AD]'
    $createdCount = Get-RowCount $Rows -Change 'Created'
    $deletedCount = Get-RowCount $Rows -Change 'Deleted'
    if (($BulkCreateThreshold -gt 0 -and $createdCount -ge $BulkCreateThreshold) -or
        ($BulkDeleteThreshold -gt 0 -and $deletedCount -ge $BulkDeleteThreshold)) {
        $prefix = '[AD] [BULK]'
    }

    if ($createdOnly) {
        $parts = @(foreach ($type in $Types) {
            $count = Get-RowCount $Rows -Type $type
            if ($count -gt 0) {
                $def = $script:TypeDefinitions[$type]
                if ($count -eq 1) { "1 $($def.Singular)" } else { "$count $($def.Plural.ToLower())" }
            }
        })
        if ($total -eq 1) { $noun = 'object' } else { $noun = 'objects' }
        return "$prefix $total new $noun ($($parts -join ', ')) - $DomainName"
    }

    $parts = @(foreach ($change in $Changes) {
        $count = Get-RowCount $Rows -Change $change
        if ($count -gt 0) { "$count $($script:ChangeDefinitions[$change].Verb)" }
    })
    return "$prefix $($parts -join ', ') - $DomainName"
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
    $csv = $Rows | Sort-Object Change, Type, When | Select-Object Change, Type, Name, AccountName, OuPath, ParentDN,
        @{ n = 'When'; e = { $_.When.ToString('yyyy-MM-dd HH:mm:ss', $script:Invariant) } },
        @{ n = 'ChangedBy'; e = { $_.Actor } },
        @{ n = 'ChangedBySource'; e = { $_.ActorSource } },
        @{ n = 'AccountCreated'; e = { if ($_.AccountCreated) { $_.AccountCreated.ToString('yyyy-MM-dd', $script:Invariant) } else { '' } } },
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

Write-Log '=== AD change report ==='
Write-Log "Changes: $($ChangeTypes -join ', ')"
Write-Log "Object types: $($ObjectTypes -join ', ')"

$trackAccounts = ($ChangeTypes -contains 'Disabled') -or ($ChangeTypes -contains 'Enabled')
$accountTypes  = @($ObjectTypes | Where-Object { $AccountTypes -contains $_ })
if ($trackAccounts -and $accountTypes.Count -eq 0) {
    Write-Log 'Disabled/Enabled only apply to User and Computer objects; none selected.' 'WARN'
}

# Validate email settings before doing any work
$apiKey = ''
if (-not $PreviewHtmlPath) {
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

$rows        = @()
$reported    = @{}
$newSnapshot = @{}
$auditNote   = ''

if ($PreviewSampleData) {
    Write-Log 'Using sample data (AD will not be queried).' 'WARN'
    $domainName     = 'corp.example.co.uk'
    $Server         = 'DC01.corp.example.co.uk'
    $periodStartUtc = $runStartUtc.AddHours(-1)
    $rows = Get-SampleRows -Changes $ChangeTypes -Types $ObjectTypes
    if ($CreatorLookup -ne 'None') { $auditNote = 'Who made each change is read from DC Security logs, falling back to the object owner for created objects.' }
}
else {
    try {
        Import-Module ActiveDirectory -ErrorAction Stop -Verbose:$false
        if (-not $Server) { $Server = (Get-ADDomain).PDCEmulator }
        $adDomain         = Get-ADDomain -Server $Server
        $domainName       = $adDomain.DNSRoot
        $DeletedObjectsDN = $adDomain.DeletedObjectsContainer
    }
    catch {
        Write-Log "Could not load the ActiveDirectory module or contact a DC: $($_.Exception.Message)" 'ERROR'
        exit 1
    }
    Write-Log "Domain: $domainName, querying $Server"

    $state = Read-State -Path $StatePath
    $previousSnapshot = @{}
    if ($state) {
        $periodStartUtc   = $state.LastRunUtc
        $windowStartUtc   = $state.LastRunUtc.AddMinutes(-$OverlapMinutes)
        $reported         = $state.Reported
        $previousSnapshot = $state.Snapshot
        Write-Log ('Last run: {0:yyyy-MM-dd HH:mm:ss} UTC, searching from {1:yyyy-MM-dd HH:mm:ss} UTC' -f $state.LastRunUtc, $windowStartUtc)
    }
    else {
        $periodStartUtc = $runStartUtc.AddHours(-$InitialLookbackHours)
        $windowStartUtc = $periodStartUtc
        Write-Log "No previous state found, looking back $InitialLookbackHours hour(s)." 'WARN'
    }

    # --- Created ---
    if ($ChangeTypes -contains 'Created') {
        foreach ($type in $ObjectTypes) {
            try { $found = Get-CreatedADObjects -Type $type -SinceUtc $windowStartUtc }
            catch {
                Write-Log "AD query for created $type objects failed: $($_.Exception.Message)" 'ERROR'
                exit 1
            }
            $count = 0
            foreach ($obj in $found) {
                if ($reported.ContainsKey("Created|$($obj.ObjectGUID)")) { continue }
                if (-not (Test-InScope ([string]$obj.DistinguishedName))) { continue }
                $rows += ConvertFrom-LiveObject -Change 'Created' -Type $type -AdObject $obj -When $obj.whenCreated
                $count++
            }
            Write-Log "Created $type objects: $count"
        }
    }

    # --- Deleted ---
    if ($ChangeTypes -contains 'Deleted') {
        foreach ($type in $ObjectTypes) {
            try { $found = Get-DeletedADObjects -Type $type -SinceUtc $windowStartUtc }
            catch {
                Write-Log "AD query for deleted $type objects failed (does this account have read access to $DeletedObjectsDN?): $($_.Exception.Message)" 'ERROR'
                exit 1
            }
            $count = 0
            foreach ($obj in $found) {
                if ($reported.ContainsKey("Deleted|$($obj.ObjectGUID)")) { continue }
                $parent = [string](Get-PropertyValue $obj 'lastKnownParent')
                if (-not (Test-InScope "CN=x,$parent")) { continue }
                $name = [string](Get-PropertyValue $obj 'msDS-LastKnownRDN')
                if (-not $name) { $name = ([string]$obj.Name -split "`n")[0] }
                $rows += New-ChangeRow -Change 'Deleted' -Type $type -Name $name `
                    -AccountName ([string](Get-PropertyValue $obj 'sAMAccountName')) `
                    -ParentDN $parent -When $obj.whenChanged -Guid ([string]$obj.ObjectGUID) `
                    -Sid ([string](Get-PropertyValue $obj 'objectSid'))
                $count++
            }
            Write-Log "Deleted $type objects: $count"
        }
    }

    # --- Disabled / Enabled (snapshot comparison) ---
    if ($trackAccounts) {
        foreach ($type in $accountTypes) {
            try { $current = Get-DisabledADObjects -Type $type }
            catch {
                Write-Log "AD query for disabled $type accounts failed: $($_.Exception.Message)" 'ERROR'
                exit 1
            }
            $set = @{}
            foreach ($obj in $current) { $set[[string]$obj.ObjectGUID] = $true }
            $newSnapshot[$type] = $set

            if (-not $previousSnapshot.ContainsKey($type)) {
                Write-Log "Recorded baseline of $($set.Count) disabled $type account(s). Disabled/enabled changes are reported from the next run." 'WARN'
                continue
            }
            $previous = $previousSnapshot[$type]

            if ($ChangeTypes -contains 'Disabled') {
                $count = 0
                foreach ($obj in $current) {
                    if ($previous.ContainsKey([string]$obj.ObjectGUID)) { continue }
                    # Accounts created disabled are new accounts, not disable actions
                    if ((ConvertTo-LocalDateTime $obj.whenCreated).ToUniversalTime() -ge $windowStartUtc) { continue }
                    if (-not (Test-InScope ([string]$obj.DistinguishedName))) { continue }
                    $when = Get-AttributeChangeTime ([string]$obj.DistinguishedName) 'userAccountControl'
                    if (-not $when) { $when = $obj.whenChanged }
                    $rows += ConvertFrom-LiveObject -Change 'Disabled' -Type $type -AdObject $obj -When $when
                    $count++
                }
                Write-Log "Disabled $type accounts: $count"
            }

            if ($ChangeTypes -contains 'Enabled') {
                $count = 0
                foreach ($guid in @($previous.Keys)) {
                    if ($set.ContainsKey($guid)) { continue }
                    # No longer in the disabled list: enabled, deleted, or moved out of scope
                    try {
                        $obj = Get-ADObject -Identity $guid -Server $Server -Properties whenCreated, whenChanged, sAMAccountName, objectSid, userAccountControl
                    }
                    catch { continue }
                    if (([int]$obj.userAccountControl -band 2) -ne 0) { continue }
                    if (-not (Test-InScope ([string]$obj.DistinguishedName))) { continue }
                    $when = Get-AttributeChangeTime ([string]$obj.DistinguishedName) 'userAccountControl'
                    if (-not $when) { $when = $obj.whenChanged }
                    $rows += ConvertFrom-LiveObject -Change 'Enabled' -Type $type -AdObject $obj -When $when -AccountCreated $obj.whenCreated
                    $count++
                }
                Write-Log "Enabled $type accounts: $count"
            }
        }
    }
}

$rows  = @($rows)
$total = $rows.Count
Write-Log "Total changes: $total"

# Work out who made each change
if (-not $PreviewSampleData -and $total -gt 0 -and $CreatorLookup -ne 'None') {
    $audit = $null
    if ($CreatorLookup -eq 'Auto' -or $CreatorLookup -eq 'AuditLog') {
        try {
            if ($DomainControllers.Count -eq 0) {
                $DomainControllers = @(Get-ADDomainController -Filter * -Server $Server |
                    Where-Object { -not $_.IsReadOnly } | ForEach-Object { $_.HostName })
            }
            $foundChanges = @($ChangeTypes | Where-Object { (Get-RowCount $rows -Change $_) -gt 0 })
            $audit = Get-AuditEvents -Computers $DomainControllers -Changes $foundChanges -Types $ObjectTypes -SinceUtc $windowStartUtc
            $auditNote = 'Who made each change is read from DC Security logs'
            if ($CreatorLookup -eq 'Auto' -and $ChangeTypes -contains 'Created') { $auditNote += ', falling back to the object owner for created objects' }
            $auditNote += '.'
            if ($audit.FailedDCs.Count -gt 0) {
                $auditNote += ' Audit log could not be read on: ' + ($audit.FailedDCs -join ', ') + '.'
            }
        }
        catch {
            Write-Log "Audit log lookup failed: $($_.Exception.Message)" 'WARN'
            $auditNote = 'Audit logs could not be read; created objects show the object owner where available.'
        }
    }
    else {
        $auditNote = 'Created by is the object owner (audit logs not checked).'
    }
    Set-RowActors -Rows $rows -Audit $audit -Mode $CreatorLookup
    $unknown = @($rows | Where-Object { $_.Actor -eq 'Unknown' }).Count
    if ($unknown -gt 0) { Write-Log "$unknown change(s) where the account responsible is unknown" 'WARN' }
}

# State to save after a successful run: drop keys older than the next window
function Save-RunState {
    if ($isPreview) { return }
    $cutoff = $runStartUtc.AddMinutes(-$OverlapMinutes)
    $next   = @{}
    foreach ($key in $reported.Keys) {
        if ($reported[$key] -ge $cutoff) { $next[$key] = $reported[$key] }
    }
    foreach ($row in $rows) {
        if (($row.Change -eq 'Created' -or $row.Change -eq 'Deleted') -and $row.WhenUtc -ge $cutoff) {
            $next["$($row.Change)|$($row.Guid)"] = $row.WhenUtc
        }
    }
    try {
        Save-State -Path $StatePath -LastRunUtc $runStartUtc -Reported $next -Snapshot $newSnapshot
    }
    catch {
        Write-Log "Could not write state file '$StatePath': $($_.Exception.Message)" 'ERROR'
        exit 1
    }
}

if ($total -eq 0 -and -not $SendWhenEmpty -and -not $PreviewHtmlPath) {
    Write-Log 'Nothing to report, no email sent.' 'OK'
    Save-RunState
    exit 0
}

# Truncation and CSV attachment
$truncated = $false
foreach ($change in $ChangeTypes) {
    foreach ($type in $ObjectTypes) {
        if ((Get-RowCount $rows -Change $change -Type $type) -gt $MaxRowsPerType) { $truncated = $true }
    }
}
$attachCsv = ($total -gt 0) -and ($truncated -or $AlwaysAttachCsv)

$html = Build-ReportHtml -Rows $rows -Changes $ChangeTypes -Types $ObjectTypes -DomainName $domainName -QueryServer $Server `
    -PeriodStartUtc $periodStartUtc -PeriodEndUtc $runStartUtc -MaxRows $MaxRowsPerType `
    -BulkCreateThreshold $BulkWarningThreshold -BulkDeleteThreshold $BulkDeleteThreshold -CsvAttached $attachCsv `
    -ShowActor ($CreatorLookup -ne 'None') -AuditNote $auditNote
$subject = Get-ReportSubject -Rows $rows -Changes $ChangeTypes -Types $ObjectTypes -DomainName $domainName `
    -BulkCreateThreshold $BulkWarningThreshold -BulkDeleteThreshold $BulkDeleteThreshold

if ($PreviewHtmlPath) {
    [IO.File]::WriteAllText($PreviewHtmlPath, $html, (New-Object Text.UTF8Encoding($false)))
    Write-Log "Subject would be: $subject"
    Write-Log "Preview written to $PreviewHtmlPath (no email sent, state not updated)." 'OK'
    exit 0
}

$attachments = @()
if ($attachCsv) {
    $fileName = 'ad-changes-{0}.csv' -f $runStartUtc.ToLocalTime().ToString('yyyyMMdd-HHmm', $Invariant)
    $attachments = @(New-CsvAttachment -Rows $rows -FileName $fileName)
    Write-Log "Attaching $fileName"
}

Write-Log "Sending: $subject"
$sent = Send-BrevoEmail -ApiKey $apiKey -To $EmailTo -From $EmailFrom -FromName $EmailFromName `
    -Subject $subject -HtmlContent $html -Attachments $attachments

if (-not $sent) {
    Write-Log 'State not updated, these changes will be included in the next run.' 'WARN'
    exit 1
}

Save-RunState
Write-Log 'Done.' 'OK'
exit 0
