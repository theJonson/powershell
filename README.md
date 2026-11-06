# powershell

A collection of PowerShell scripts for Windows administration.

| Script | Purpose |
| --- | --- |
| [`ad-new-object-report.ps1`](ad-new-object-report.ps1) | Emails IT a digest of newly created Active Directory objects via the Brevo API |
| [`b2-sync-claude.ps1`](b2-sync-claude.ps1) | Syncs a local folder to Backblaze B2 and emails a summary via Brevo |

---

## ad-new-object-report.ps1

Runs on a schedule (hourly is recommended) and sends a single email listing every Active Directory object created since the previous run. For each object it shows:

- the object's name (and account name, where it has one)
- the OU path it was created in
- the date and time it was created
- who created it

By default it reports **users** only. Groups, computers, contacts and organisational units can be added with `-ObjectTypes`.

Example emails are in the [`examples`](examples) folder.

### Requirements

- Windows PowerShell 5.1 or PowerShell 7+
- The ActiveDirectory module (RSAT, or run it on a domain controller)
- A Brevo account with an API key and a verified sender address
- An account that can read Active Directory (a normal domain user is enough to read objects; reading DC audit logs needs the extra access described in [Showing who created an object](#showing-who-created-an-object))

### Quick start

1. Copy the script to the server that will run it, for example `C:\Scripts\ad-new-object-report.ps1`.

2. Preview the email template with built-in sample data. This does not touch Active Directory or Brevo:

   ```powershell
   .\ad-new-object-report.ps1 -PreviewSampleData -ObjectTypes User,Group,Computer -PreviewHtmlPath C:\Temp\preview.html
   ```

3. Preview a real report from Active Directory without sending an email or saving any state:

   ```powershell
   .\ad-new-object-report.ps1 -InitialLookbackHours 168 -PreviewHtmlPath C:\Temp\preview.html
   ```

4. Store the Brevo API key encrypted. **Run this as the account the scheduled task will use**, because the file can only be decrypted by that account on that machine:

   ```powershell
   Read-Host -AsSecureString -Prompt 'Brevo API key' | Export-Clixml -Path C:\Scripts\brevo.key.xml
   ```

5. Send a test email using the sample data:

   ```powershell
   .\ad-new-object-report.ps1 -PreviewSampleData -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk
   ```

6. Create the scheduled task (see [Scheduling](#scheduling)).

### Common examples

Report new users (the default):

```powershell
.\ad-new-object-report.ps1 -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk
```

Report users, groups and computers to two recipients:

```powershell
.\ad-new-object-report.ps1 -ObjectTypes User,Group,Computer -EmailTo it@example.co.uk,helpdesk@example.co.uk -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailFrom alerts@example.co.uk
```

Ignore OUs that are filled by automated processes (separate multiple OUs with a semicolon, because OU paths contain commas):

```powershell
.\ad-new-object-report.ps1 -ObjectTypes User,Computer -ExcludeOU 'OU=Autopilot,OU=Devices,DC=corp,DC=example,DC=co,DC=uk;OU=Leavers,DC=corp,DC=example,DC=co,DC=uk' -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk
```

Daily heartbeat email, sent even when nothing new was found, so you know the job is still running:

```powershell
.\ad-new-object-report.ps1 -SendWhenEmpty -StatePath C:\Scripts\heartbeat.state.json -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk -EmailFrom alerts@example.co.uk
```

Give the heartbeat task its own `-StatePath`, so it does not share state with the hourly task. Because it keeps its own state, the daily email also lists everything created in the last day, so it doubles as a daily summary.

### Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `-ObjectTypes` | `User` | Any of `User`, `Group`, `Computer`, `Contact`, `OrganizationalUnit`, separated by commas |
| `-EmailTo` | | One or more recipients, separated by commas or semicolons. All recipients can see each other's addresses |
| `-EmailFrom` | | Sender address. It must be a verified sender in Brevo |
| `-EmailFromName` | `AD Change Monitor` | Sender display name |
| `-BrevoApiKeyPath` | | Path to the encrypted key file created in step 4 of the quick start |
| `-BrevoApiKey` | | Plain-text API key. Avoid this for scheduled tasks |
| `-SearchBase` | whole domain | Limit the search to one OU |
| `-ExcludeOU` | | OUs to ignore (including everything below them), separated by semicolons |
| `-Server` | PDC emulator | Domain controller to query for objects |
| `-CreatorLookup` | `Auto` | `Auto` (audit log, then object owner), `AuditLog`, `Owner` or `None` |
| `-DomainControllers` | all writable DCs | DCs whose Security logs are searched for creation events |
| `-StatePath` | next to the script | JSON file that records the last run |
| `-InitialLookbackHours` | `24` | How far back to look on the first run |
| `-OverlapMinutes` | `60` | Re-check window to catch objects that replicate late |
| `-MaxRowsPerType` | `50` | Rows shown per object type before the full list moves to a CSV attachment |
| `-BulkWarningThreshold` | `20` | Show a bulk creation warning at this many objects in one run. `0` turns it off |
| `-SendWhenEmpty` | off | Send an email even when nothing new was found |
| `-AlwaysAttachCsv` | off | Always attach the CSV, not only when the email is truncated |
| `-PreviewHtmlPath` | | Write the email to an HTML file instead of sending it. State is not saved |
| `-PreviewSampleData` | off | Use built-in sample objects instead of Active Directory. State is not saved |
| `-LogPath` | | Append log lines to this file |

Full help, including all of the above, is available with:

```powershell
Get-Help .\ad-new-object-report.ps1 -Full
```

### How it works

- Each run searches Active Directory for objects whose creation time (`whenCreated`) is after the previous run. It always queries the same domain controller, the PDC emulator by default.
- The last run time and the IDs of recently reported objects are saved in a small JSON state file. Each run also re-checks the 60 minutes before the last run, which catches objects that took a while to replicate. The saved IDs stop anything being reported twice.
- The state file is only updated after the email has been sent. If sending fails, the same objects are included in the next run.
- On the first run there is no state file, so it looks back `-InitialLookbackHours` (24 hours by default).
- All new objects in a run go into **one email**, grouped by object type and in the order they were created. The subject line includes the counts, for example `[AD] 6 new objects (3 users, 1 group, 2 computers) - corp.example.co.uk`.
- When more than `-MaxRowsPerType` objects of one type are found, the email shows the first rows and attaches a CSV file listing every object.
- When `-BulkWarningThreshold` or more objects are found in one run, the email shows a warning banner and the subject is marked `[BULK]`. An unexpected burst of new accounts is worth investigating.
- Objects created in the default `CN=Users` or `CN=Computers` containers are flagged with a **DEFAULT CONTAINER** badge, because this usually means the normal process was bypassed.
- When nothing new is found, no email is sent unless `-SendWhenEmpty` is used.
- The script exits with code `0` on success and `1` on failure, so failed runs show in Task Scheduler.

### Showing who created an object

Active Directory does not store who created an object, so the script reads it from the Security event log on the domain controller where the object was created. With the default `-CreatorLookup Auto`:

1. It searches each writable DC's Security log for creation events and matches them to the new objects.
2. If no event is found, it shows the object's **owner** instead. The owner is the creator when a delegated (non-admin) account created the object, but it is just "Domain Admins" when any Domain Admin created it.
3. If neither is available, it shows **Unknown**.

The email always shows where each name came from (for example "audit log on DC01" or "object owner").

The following needs to be in place for the audit log lookup to work.

#### 1. Audit policy on the domain controllers

In a Group Policy linked to the **Domain Controllers** OU (for example the Default Domain Controllers Policy), go to:

`Computer Configuration > Policies > Windows Settings > Security Settings > Advanced Audit Policy Configuration > Audit Policies`

| Category | Subcategory | Setting | Events |
| --- | --- | --- | --- |
| Account Management | Audit User Account Management | Success | 4720 |
| Account Management | Audit Computer Account Management | Success | 4741 |
| Account Management | Audit Security Group Management | Success | 4727, 4731, 4754 |
| Account Management | Audit Distribution Group Management | Success | 4744, 4749, 4759 |
| DS Access | Audit Directory Service Changes | Success | 5137 (contacts and OUs only) |

Also enable this setting under `Security Settings > Local Policies > Security Options`:

**Audit: Force audit policy subcategory settings (Windows Vista or later) to override audit policy category settings**

Without it, older category-level audit settings can silently override the settings above.

Check the result on each DC with:

```powershell
auditpol /get /category:"Account Management","DS Access"
```

#### 2. Auditing entry for contacts and OUs

This step is only needed if you report on contacts or OUs, because those have no account management events. Event 5137 is only logged when auditing is set on the parent object. In ADSI Edit, or in Active Directory Users and Computers with Advanced Features turned on, open the **domain root** and go to:

`Properties > Security > Advanced > Auditing > Add`

- Principal: **Everyone**
- Type: **Success**
- Applies to: **This object and all descendant objects**
- Permission: **Create all child objects**

#### 3. Security log size

The events must still be in the log when the script runs. If the log fills up and overwrites them, the creator shows as unknown. Increase the Security log maximum size on the DCs (1 GB is a sensible starting point) and keep the schedule hourly:

```powershell
wevtutil sl Security /ms:1073741824
```

For a permanent setting, use Group Policy: `Computer Configuration > Policies > Windows Settings > Security Settings > Event Log`.

#### 4. Remote log access for the script's account

- Add the account that runs the scheduled task to the domain's built-in **Event Log Readers** group.
- Enable the **Remote Event Log Management** inbound firewall rules on the DCs.

Test as that account:

```powershell
Get-WinEvent -ComputerName DC01 -LogName Security -MaxEvents 1
```

If a DC's log cannot be read, the run carries on and the email footer names that DC.

If you do not want to set up auditing, use `-CreatorLookup Owner` to show only the object owner, or `-CreatorLookup None` to remove the column.

### Scheduling

Create the task as the service account that will run it. That account must also be the one that created the encrypted API key file.

```powershell
$script  = 'C:\Scripts\ad-new-object-report.ps1'
$arg     = "-NoProfile -ExecutionPolicy Bypass -File `"$script`" -ObjectTypes User,Group,Computer -BrevoApiKeyPath C:\Scripts\brevo.key.xml -EmailTo it@example.co.uk,helpdesk@example.co.uk -EmailFrom alerts@example.co.uk -LogPath C:\Scripts\ad-new-object-report.log"
$action  = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arg
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Hours 1)
Register-ScheduledTask -TaskName 'AD New Object Report' -Action $action -Trigger $trigger -User 'DOMAIN\svc-adreport' -Password '<password>' -RunLevel Limited
```

The script accepts comma-separated lists, such as `-ObjectTypes User,Group` and `-EmailTo a@example.co.uk,b@example.co.uk`, even when run with `powershell.exe -File`. Normally `-File` passes these through as a single value.

### Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `Could not read Brevo API key` | The key file was created by a different account or on a different machine. Recreate it as the task's account |
| `Failed to send email via Brevo` | Wrong API key, unverified sender address, or the server cannot reach `api.brevo.com` on port 443. The Brevo error message is in the log |
| Created by shows **Unknown** | Auditing is not enabled for that object type, the event has been overwritten in the log, or the DC could not be read |
| Created by shows **Domain Admins** | No audit event was found, so the object owner was used instead, and a Domain Admin created the object |
| The same objects are reported again | The previous email failed to send, so the state file was not updated. This is by design |
| Nothing is reported on the first run | Nothing was created in the last `-InitialLookbackHours`. Increase it for a one-off test |
