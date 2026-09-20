param(
    [Parameter(Mandatory=$true)][string]$SourcePath,
    [Parameter(Mandatory=$true)][string]$DestPath,
    [int]$Padding = 12,
    [double]$UpscaleFactor = 2.0
)

Add-Type -AssemblyName System.Drawing

$bmp = [System.Drawing.Bitmap]::FromFile($SourcePath)

# Sample the background color from a corner that's reliably empty page background.
$bg = $bmp.GetPixel(2, $bmp.Height - 2)
$tolerance = 18

function IsBackground($c) {
    return ([math]::Abs([int]$c.R - [int]$bg.R) -le $tolerance) -and
           ([math]::Abs([int]$c.G - [int]$bg.G) -le $tolerance) -and
           ([math]::Abs([int]$c.B - [int]$bg.B) -le $tolerance)
}

$minX = $bmp.Width
$maxX = 0
$minY = $bmp.Height
$maxY = 0

# Coarse scan (every 2px) for speed, then we pad generously.
for ($y = 0; $y -lt $bmp.Height; $y += 2) {
    for ($x = 0; $x -lt $bmp.Width; $x += 2) {
        $p = $bmp.GetPixel($x, $y)
        if (-not (IsBackground $p)) {
            if ($x -lt $minX) { $minX = $x }
            if ($x -gt $maxX) { $maxX = $x }
            if ($y -lt $minY) { $minY = $y }
            if ($y -gt $maxY) { $maxY = $y }
        }
    }
}

$minX = [math]::Max(0, $minX - $Padding)
$minY = [math]::Max(0, $minY - $Padding)
$maxX = [math]::Min($bmp.Width - 1, $maxX + $Padding)
$maxY = [math]::Min($bmp.Height - 1, $maxY + $Padding)

$cropW = $maxX - $minX
$cropH = $maxY - $minY

$cropRect = New-Object System.Drawing.Rectangle($minX, $minY, $cropW, $cropH)
$cropped = New-Object System.Drawing.Bitmap($cropW, $cropH)
$g = [System.Drawing.Graphics]::FromImage($cropped)
$g.DrawImage($bmp, (New-Object System.Drawing.Rectangle(0, 0, $cropW, $cropH)), $cropRect, [System.Drawing.GraphicsUnit]::Pixel)
$g.Dispose()
$bmp.Dispose()

$finalW = [int]($cropW * $UpscaleFactor)
$finalH = [int]($cropH * $UpscaleFactor)
$final = New-Object System.Drawing.Bitmap($finalW, $finalH)
$g2 = [System.Drawing.Graphics]::FromImage($final)
$g2.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
$g2.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
$g2.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
$g2.DrawImage($cropped, 0, 0, $finalW, $finalH)
$g2.Dispose()
$cropped.Dispose()

$final.Save($DestPath, [System.Drawing.Imaging.ImageFormat]::Png)
$final.Dispose()

Write-Output "crop: ($minX,$minY)-($maxX,$maxY) -> ${cropW}x${cropH} -> ${finalW}x${finalH}: $DestPath"
