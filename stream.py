import boto3
import subprocess
import time

# AWS IVS client
ivs_client = boto3.client('ivs')

# Path to the video file you want to stream
video_file_path = './football.mp4'

# Number of channels to create
num_channels = 10  # Adjust this to 1000 for your full test

def create_channel(channel_name):
    response = ivs_client.create_channel(
        name=channel_name,
        latencyMode='LOW',  # or 'NORMAL'
        type='STANDARD'     # or 'BASIC'
    )
    return response['channel']['ingestEndpoint'], response['streamKey']['value']
    
def get_existing_channels():
    channels = []
    paginator = ivs_client.get_paginator('list_channels')
    for page in paginator.paginate():
        for channel in page['channels']:
            channel_name = channel['name']
            channel_arn = channel['arn']
            response = ivs_client.get_channel(arn=channel_arn)
            ingest_endpoint = response['channel']['ingestEndpoint']
            stream_key_arn = ivs_client.list_stream_keys(channelArn=channel_arn)['streamKeys'][0]['arn']
            stream_key = ivs_client.get_stream_key(arn=stream_key_arn)['streamKey']['value']
            channels.append((channel_name, ingest_endpoint, stream_key))
    return channels

def stream_to_channel(ingest_endpoint, stream_key, channel_number):
    rtmp_url = f"rtmps://{ingest_endpoint}:443/app/{stream_key}"
    ffmpeg_command = [
        'ffmpeg', '-re', '-i', video_file_path, '-c:v', 'libx264', '-preset', 'veryfast', '-maxrate', '3000k',
        '-bufsize', '6000k', '-f', 'flv', rtmp_url
    ]
    print(ffmpeg_command)
    print(f"Starting stream for Channel-{channel_number} to {rtmp_url}")
    subprocess.Popen(ffmpeg_command)

def main():
    # Step 1: Get existing channels
    existing_channels = get_existing_channels()
    print(f"Found {len(existing_channels)} existing channels.")

    channels = existing_channels.copy()

    # Step 2: Create new channels if needed
    for i in range(len(existing_channels) + 1, num_channels + 1):
        channel_name = f"Channel-{i}"
        ingest_endpoint, stream_key = create_channel(channel_name)
        channels.append((channel_name, ingest_endpoint, stream_key))
        print(f"Created {channel_name} with ingest endpoint: {ingest_endpoint}")

    time.sleep(10)  # Short delay to ensure channels are ready

    # Step 3: Stream to channels
    for idx, (channel_name, ingest_endpoint, stream_key) in enumerate(channels[1:10]):
        stream_to_channel(ingest_endpoint, stream_key, idx + 1)
        time.sleep(2)  # Slight delay between starting streams (adjust if needed)

    print("All streams started.")

if __name__ == "__main__":
    main()

