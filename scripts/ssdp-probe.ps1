#Requires -Version 5.1
<#
    Asks the LAN the question Bilibili's phone asks it: which targets answer, with
    which LOCATION, SERVER and BOOTID headers. Reads each answered descriptor back
    and prints the compatibility fields the NVA classifier depends on.

    Run this on the PC while nva2dlna is running, from the same subnet as the phone.
    It sends unicast replies to an ephemeral port, so it never fights the server for
    UDP 1900. Pass -Notify to watch ssdp:alive/byebye churn instead, which does need
    the server stopped.

    -Peer 192.168.1.5[:1900] skips multicast entirely and asks one host directly.
    Use it to tell a wire-format problem apart from a multicast or firewall problem:
    if -Peer answers but plain discovery is silent, the bytes are fine and the LAN
    path is not.

    -Raw prints each datagram byte-for-byte, and -FromFile replays a capture or a
    -Raw dump (packets separated by a blank line) through the same analysis.
#>
[CmdletBinding()]
param(
    [string[]]$SearchTarget = @(
        'ssdp:all',
        'urn:schemas-upnp-org:device:MediaRenderer:1',
        'urn:schemas-upnp-org:service:AVTransport:1',
        'urn:schemas-upnp-org:service:RenderingControl:1',
        'urn:schemas-upnp-org:service:ConnectionManager:1',
        'urn:app-bilibili-com:service:NirvanaControl:3',
        'urn:schemas-upnp-org:service:NirvanaControl:3'
    ),
    [int]$Seconds = 4,
    [string]$Interface,
    [string]$Peer,
    [string]$FromFile,
    [switch]$Raw,
    [switch]$Notify
)

$ErrorActionPreference = 'Stop'
$Group = [System.Net.IPAddress]::Parse('239.255.255.250')
$Port = 1900

function Open-Socket {
    param([int]$BindPort)

    # UdpClient rather than Socket because it is the one with an interface-aware
    # JoinMulticastGroup overload on .NET Framework 4.x.
    $client = [System.Net.Sockets.UdpClient]::new([System.Net.Sockets.AddressFamily]::InterNetwork)
    $client.Client.SetSocketOption(
        [System.Net.Sockets.SocketOptionLevel]::Socket,
        [System.Net.Sockets.SocketOptionName]::ReuseAddress, $true)
    if ($BindPort -gt 0) { $client.Client.Bind([System.Net.IPEndPoint]::new(
        [System.Net.IPAddress]::Any, $BindPort)) }
    if ($Peer) { return $client }
    if ($Interface) {
        $local = [System.Net.IPAddress]::Parse($Interface)
        $client.JoinMulticastGroup($Group, $local)
        $client.Client.SetSocketOption(
            [System.Net.Sockets.SocketOptionLevel]::Ip,
            [System.Net.Sockets.SocketOptionName]::MulticastInterface,
            [byte[]]$local.GetAddressBytes())
    } else {
        $client.JoinMulticastGroup($Group)
    }
    $client
}

function Resolve-Endpoint {
    param([string]$Text, [int]$DefaultPort)

    $address = $Text
    $port = $DefaultPort
    $split = $Text.LastIndexOf(':')
    if ($split -gt 0) {
        $address = $Text.Substring(0, $split)
        $port = [int]$Text.Substring($split + 1)
    }
    [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Parse($address), $port)
}

function Read-Headline {
    param([string]$FirstLine, [hashtable]$Headers)

    if ($FirstLine -like 'NOTIFY *') {
        return 'NOTIFY ' + (Get-Header $Headers 'NTS')
    }
    'M-SEARCH reply'
}

function Get-Header {
    param([hashtable]$Headers, [string]$Name)

    foreach ($key in $Headers.Keys) {
        if ($key -ieq $Name) { return $Headers[$key] }
    }
    ''
}

function Parse-Response {
    param([string]$Text, [string]$Remote, [int]$ElapsedMs)

    $lines = $Text -split "`r?`n"
    $headers = @{}
    foreach ($line in $lines | Select-Object -Skip 1) {
        $split = $line.IndexOf(':')
        if ($split -gt 0) {
            $headers[$line.Substring(0, $split).Trim()] = $line.Substring($split + 1).Trim()
        }
    }
    [pscustomobject]@{
        FirstLine = $lines[0]
        Headers   = $headers
        Remote    = $Remote
        Raw       = $Text
        ElapsedMs = $ElapsedMs
    }
}

function Receive-Packet {
    param([System.Net.Sockets.UdpClient]$Client)

    $buffer = [byte[]]::new(65536)
    $remote = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Any, 0)
    try {
        $count = $Client.Client.ReceiveFrom($buffer, [ref]$remote)
    } catch [System.Net.Sockets.SocketException] {
        if ($_.Exception.SocketErrorCode -eq 'TimedOut') { return $null }
        throw
    }
    Parse-Response -Text ([System.Text.Encoding]::UTF8.GetString($buffer, 0, $count)) `
        -Remote ($remote.Address.ToString() + ':' + $remote.Port) -ElapsedMs 0
}

$packets = [System.Collections.Generic.List[object]]::new()

if ($FromFile) {
    # Replay a capture: packets separated by a blank line, exactly as -Raw prints them.
    $dump = [System.IO.File]::ReadAllText((Resolve-Path -LiteralPath $FromFile))
    $elapsed = 0
    foreach ($block in ($dump -split "(`r?`n){2,}")) {
        if ($block.Trim()) {
            $elapsed += 5
            $packets.Add((Parse-Response -Text $block -Remote 'capture' -ElapsedMs $elapsed)) | Out-Null
        }
    }
} else {
$bindPort = if ($Notify) { $Port } else { 0 }
$client = Open-Socket -BindPort $bindPort
$destination = if ($Peer) {
    Resolve-Endpoint -Text $Peer -DefaultPort $Port
} else {
    [System.Net.IPEndPoint]::new($Group, $Port)
}
$deadline = (Get-Date).AddSeconds($Seconds)
$watch = [System.Diagnostics.Stopwatch]::StartNew()

try {
    $encoder = [System.Text.Encoding]::ASCII
    foreach ($st in $SearchTarget) {
        $request = "M-SEARCH * HTTP/1.1`r`nHOST: 239.255.255.250:1900`r`n" +
            'MAN: "ssdp:discover"' + "`r`nMX: 1`r`nST: $st`r`n`r`n"
        $bytes = $encoder.GetBytes($request)
        $client.Send($bytes, $bytes.Length, $destination) | Out-Null
    }

    $client.Client.ReceiveTimeout = 250
    while ((Get-Date) -lt $deadline) {
        $packet = Receive-Packet -Client $client
        if ($null -ne $packet) {
            $packet.ElapsedMs = [int]$watch.ElapsedMilliseconds
            $packets.Add($packet) | Out-Null
        }
    }
} finally {
    $client.Close()
}
}

if ($packets.Count -eq 0) {
    Write-Host 'No SSDP answers at all. Check the PC is on the phone''s subnet, that'
    Write-Host 'nva2dlna is running, and that Windows Firewall is not dropping inbound'
    Write-Host 'UDP for this script (a Public network profile will). Then retry with'
    Write-Host "-Peer <this PC's address> to ask the server directly and rule multicast out."
    exit 1
}

$selected = [System.Collections.Generic.List[object]]::new()
foreach ($packet in $packets) {
    $st = Get-Header $packet.Headers 'ST'
    $nt = Get-Header $packet.Headers 'NT'
    $target = if ($st) { $st } elseif ($nt) { $nt } else { '' }
    if (-not $Notify -and $target -notmatch
        'MediaRenderer|MediaServer|NirvanaControl|AVTransport|RenderingControl|ConnectionManager|rootdevice') {
        continue
    }
    $selected.Add([pscustomobject]@{
        ElapsedMs = $packet.ElapsedMs
        Kind      = Read-Headline -FirstLine $packet.FirstLine -Headers $packet.Headers
        From      = $packet.Remote
        ST        = $target
        USN       = Get-Header $packet.Headers 'USN'
        Location  = Get-Header $packet.Headers 'LOCATION'
        Server    = Get-Header $packet.Headers 'SERVER'
        BootId    = Get-Header $packet.Headers 'BOOTID.UPNP.ORG'
        ConfigId  = Get-Header $packet.Headers 'CONFIGID.UPNP.ORG'
    }) | Out-Null
}

Write-Host ''
Write-Host ("=== {0} SSDP answers, {1} that matter ===" -f $packets.Count, $selected.Count)

# One block per distinct answer. A table would truncate SERVER, and SERVER is one of
# the things being checked here.
$groups = $selected | Group-Object Kind, ST, USN, Location, Server, BootId, ConfigId
foreach ($group in ($groups | Sort-Object { $_.Group[0].ST }, { $_.Group[0].Kind })) {
    $first = $group.Group[0]
    Write-Host ''
    Write-Host ("[{0} ms, x{1}] {2} from {3}" -f $first.ElapsedMs, $group.Count, $first.Kind, $first.From)
    Write-Host ("  ST       {0}" -f $first.ST)
    Write-Host ("  USN      {0}" -f $first.USN)
    Write-Host ("  LOCATION {0}" -f $first.Location)
    Write-Host ("  SERVER   {0}" -f $first.Server)
    if ($first.BootId) { Write-Host ("  BOOTID   {0}  CONFIGID {1}" -f $first.BootId, $first.ConfigId) }
    else { Write-Host '  BOOTID   <none>' }
}

$serverStrings = @($selected.Server | Sort-Object -Unique)
Write-Host ''
Write-Host '=== SERVER strings seen ==='
foreach ($server in $serverStrings) { Write-Host "  $server" }
if ($serverStrings.Count -gt 1) {
    Write-Host '  -> more than one: Bilibili classifies per host, so a generic second'
    Write-Host '     face can cost the NVA session. Send one string for every target.'
}

if ($Raw) {
    Write-Host ''
    Write-Host '=== every answer, exactly as it arrived ==='
    foreach ($packet in $packets) {
        Write-Host ("--- {0} ms from {1} ---" -f $packet.ElapsedMs, $packet.Remote)
        Write-Host ($packet.Raw -replace "`r`n", "`n")
    }
}

# The classification question is per-face, not per-target: Bilibili needs a
# MediaRenderer/AVTransport hit whose LOCATION leads to a Nirvana-bearing descriptor.
foreach ($location in ($selected.Location | Where-Object { $_ } | Sort-Object -Unique)) {
    Write-Host ''
    Write-Host "=== descriptor at $location ==="
    try {
        $response = Invoke-WebRequest -Uri $location -UseBasicParsing -TimeoutSec 4
        $xml = $response.Content
    } catch {
        Write-Host "could not read it: $($_.Exception.Message)"
        continue
    }
    foreach ($field in 'friendlyName', 'UDN', 'X_brandName', 'hostVersion', 'ottVersion',
        'channelName', 'capability') {
        $match = [regex]::Match($xml, "<$field>(.*?)</$field>", 'IgnoreCase')
        Write-Host ("{0,-14} {1}" -f $field, $(if ($match.Success) { $match.Groups[1].Value } else { '<missing>' }))
    }
    $services = [regex]::Matches($xml, '<serviceType>(.*?)</serviceType>', 'IgnoreCase') |
        ForEach-Object { $_.Groups[1].Value }
    Write-Host "serviceList"
    foreach ($service in $services) { Write-Host "  $service" }
    $nirvana = $services -imatch 'NirvanaControl'
    $verdict = if ($nirvana -and $xml -imatch '<capability>') {
        'NVA face (a Bilibili phone can classify this)'
    } elseif ($nirvana) {
        'Nirvana service but no <capability> - senders will hide 4K and danmaku'
    } else {
        'plain DLNA face'
    }
    Write-Host "-> $verdict"
}

Write-Host ''
Write-Host '=== NVA classification check ==='
$nvaLocations = @($selected | Where-Object { $_.ST -imatch 'NirvanaControl' }).Location |
    Sort-Object -Unique
foreach ($face in 'MediaRenderer', 'AVTransport', 'RenderingControl', 'ConnectionManager') {
    $hits = @($selected | Where-Object { $_.ST -imatch ":$($face):1$" })
    if ($hits.Count -eq 0) {
        Write-Host "no $face answer to judge (was it in -SearchTarget?)"
        continue
    }
    $locations = @($hits.Location | Sort-Object -Unique)
    foreach ($hit in $locations) {
        $tag = if ($nvaLocations -contains $hit) { 'NVA' } else { 'plain' }
        Write-Host ("{0,-20} {1,-6} {2}" -f $face, $tag, $hit)
    }
    # A control point that gets two answers for one ST keeps one LOCATION for that
    # device, so if the second face wins the match the receiver is read as generic DLNA.
    if ($locations.Count -gt 1) {
        Write-Host ("  -> {0} answered from {1} locations; if the plain one wins the match," -f $face, $locations.Count)
        Write-Host '     Bilibili downgrades the host and never opens the NVA socket'
    }
}
