import streamlit as st
import pandas as pd
import os
import urllib.request
from moviepy.editor import VideoFileClip
import requests
from google.cloud import storage
import os
import tempfile

# Streamlit Title
st.title("ELC Trends Gifs")
st.write("Interact with the Apify API to process TikTok videos, generate GIFs, and export trend data.")

# Extract the secret
gcp_secret = st.secrets["gcp_secret"]

# Write the secret to a temporary file
with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
    temp_file.write(gcp_secret)
    temp_file_path = temp_file.name

# Set the environment variable to the temporary file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

# Define API Functions
def run_actor_task(data: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    url = f"https://api.apify.com/v2/actor-tasks/H70fR5ndjUD0loq5H/runs?token=apify_api_VUQNA5xFO4IwieTeWX7HmKUYnNZOnw0c2tgk"
    response = requests.post(url, json=data, headers=headers)
    return response.json()

def get_items(dataset_id: str) -> list:
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true"
    response = requests.get(url)
    return response.json()

def convert_to_gif(media_file, max_duration=2, fps=3, output_dir='/content/gifs'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        with VideoFileClip(media_file) as clip:
            # If the video is longer than `max_duration`, use only the first `max_duration` seconds
            if clip.duration > max_duration:
                clip = clip.subclip(0, max_duration)

            clip = clip.set_fps(fps)

            output_gif_path = os.path.join(output_dir, os.path.splitext(os.path.basename(media_file))[0] + '.gif')
            clip.write_gif(output_gif_path)
            return output_gif_path
    except Exception as e:
        raise Exception(f"Failed to convert {media_file}: {e}")

# Define a list of countries
country_options = ["USA", "Canada", "UK", "Australia", "India"]  # Add your set of countries here

# Replace the text input with a dropdown selection
country_name = st.selectbox("Select Country Name", country_options)

# File Upload
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

if uploaded_file and country_name:
    # Read the Excel file
    input_df = pd.read_excel(uploaded_file, sheet_name=f"{country_name}_Links")
    st.write("length of input link",len(input_df['Links']))
    input_list_of_dicts = input_df.to_dict(orient="records")

    input_params = {
        "disableCheerioBoost": False,
        "disableEnrichAuthorStats": False,
        "resultsPerPage": 1,
        "searchSection": "/video",
        "shouldDownloadCovers": True,
        "shouldDownloadSlideshowImages": False,
        "shouldDownloadVideos": True,
        "maxProfilesPerQuery": 10,
        "tiktokMemoryMb": "default"
    }

    input_params["postURLs"] = [row["Links"] for row in input_list_of_dicts]

    st.write("Starting API Task...")
    response = run_actor_task(input_params)
    

    if "data" in response:
        dataset_id = response["data"].get("defaultDatasetId")
        st.write(f"Dataset ID: {dataset_id}")

        st.write("Fetching items from dataset...")
        dataset = get_items(dataset_id)

        source = "tiktok"

        all_items_dict = {}
        for raw_row in dataset:
            try:
                original_url = raw_row["submittedVideoUrl"]
                all_items_dict[original_url] = {
                    "Gcs Url": raw_row["gcsMediaUrls"][0]
                }
            except Exception as e:
                st.write(f"Error: {e}")

        for input_dict in input_list_of_dicts:
            clean_url = input_dict["Links"]
            video_id = clean_url.split("?")[0].split("/")[-1]
            input_dict["Gcs Url"] = f"https://storage.googleapis.com/tiktok-actor-content/{video_id}.mp4"
            input_dict["Gif Url"] = f"https://storage.googleapis.com/tiktok-actor-content/gifs_20240419/{video_id}.gif"

        output_df = pd.DataFrame(input_list_of_dicts)
        
        # Rename columns to ensure consistency
        output_df.rename(columns={"Gcs Url": "GCS URL"}, inplace=True)
        
        output_file = f"{country_name}_duration.csv"
        output_df.to_csv(output_file, index=False, encoding="utf_8_sig")
        # csv_data = output_df.to_csv(index=False, encoding="utf_8_sig")
    
    # Add a download button to allow users to download the CSV file
        st.download_button(
            label="Download Duration CSV",
            data=output_df.to_csv( index=False, encoding="utf_8_sig"),
            file_name=output_file,
            mime="text/csv"
    ) # THE ABOVE BLOCK WILL GENERTE THE GSC AND GIF URL ALSONG WITH THE DURATION DATA - pending
      # NEED TO ADD VALIDATION FOR THE SCRIPT -done 
      # able to generate the duration.csv file also  - done 

        
        #downloading the videos
        st.write("Downloading Videos...")

        df = pd.read_csv(f"{country_name}_duration.csv")
        list_of_dicts = df.to_dict(orient="records")

        # Initialize a list to keep track of failed downloads
        failed_downloads = []

        for raw_row in input_list_of_dicts:
            gcs_url = raw_row["Gcs Url"]
            input_link = raw_row["Links"]  # Store the original input link
            try:
                video_id = gcs_url.split("/")[-1].split(".")[0]
                output_file_path = f"{country_name}_tiktok_videos_all/{video_id}.mp4"
                if not os.path.exists(f"{country_name}_tiktok_videos_all"):
                    os.makedirs(f"{country_name}_tiktok_videos_all")
                st.write(f"Downloading video from {gcs_url} to {output_file_path}...")
                urllib.request.urlretrieve(gcs_url, output_file_path)
                st.write(f"Successfully downloaded {input_link}. ✅")  # Changed to input link
            except Exception as e:
                st.write(f"Error downloading {input_link}: {e} ❌")  # Changed to input link
                failed_downloads.append(input_link)  # Add the failed input link to the list

        # Print the failed download URLs at the end
        if failed_downloads:
            st.header("The following input URLs failed to download:")
            for url in failed_downloads:
                st.write(url)

        st.write("Converting Videos to GIFs...")

        df = pd.read_csv(f"{country_name}_duration.csv")
        list_of_dicts = df.to_dict(orient="records")

        df2 = pd.read_excel(uploaded_file, sheet_name=f"{country_name}_Trend details")
        trend_details_list_of_dicts = df2.to_dict(orient="records")

        trend_gif_dict = {}
        curr_trend = ""
        for raw_row in list_of_dicts:
            video_url = raw_row["GCS URL"] #change the gcs url to GCS URL
            try:
                video_id = video_url.split("/")[-1].split(".")[0]
                st.write(f"Converting {video_url} to GIF...")
                gif_path = convert_to_gif(
                    f"{country_name}_tiktok_videos_all/{video_id}.mp4",
                    max_duration=2,
                    fps=3,
                    output_dir=f"{country_name}_gifs"
                )
                
                
                trend_gif_dict[raw_row["Trend"]] = gif_path
                st.write(f"Successfully converted {video_url} to GIF at {gif_path}. ✅")

            except Exception as e:
                st.write(f"Error converting {video_url} to GIF: {e} ❌")

            # Add this block to update trend details with GIF paths
            for trend_details_dict in trend_details_list_of_dicts:
                trend = trend_details_dict["Trend"]
                trend_details_dict["Hero Tile"] = trend_gif_dict.get(trend, "")
        

            output_df = pd.DataFrame(trend_details_list_of_dicts)
            output_df.to_csv(f"{country_name}_trend_gifs.csv", index=False, encoding="utf_8_sig")

        st.write("GIF Conversion Completed!")


        #uploading gifs to gcs 
        if st.button("Upload GIFs to GCS"):
            st.write("Uploading the gifs to Gcs...")        
            def upload_folder_to_gcs(bucket_name, source_folder, destination_prefix=""):
                """
                Recursively uploads a local folder to a Google Cloud Storage bucket.

                Args:
                    bucket_name: The name of the Google Cloud Storage bucket.
                    source_folder: The path to the local folder to upload.
                    destination_prefix: The prefix to use for the uploaded files in the bucket.
                """
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)

                for root, _, files in os.walk(source_folder):
                    for file in files:
                        source_file_path = os.path.join(root, file)
                        # Construct the destination path in GCS
                        relative_path = os.path.relpath(source_file_path, source_folder)
                        destination_blob_name = os.path.join(destination_prefix, relative_path).replace("\\", "/")

                        # Upload file to GCS
                        blob = bucket.blob(destination_blob_name)
                        blob.upload_from_filename(source_file_path)
                        st.write(f"File {source_file_path} uploaded to {destination_blob_name}")

            # Ensure paths are valid
            bucket_name = "tiktok-actor-content"
            gif_folder = f"{country_name}_gifs"  # Path to the GIF folder
            destination_prefix = "gifs_20240419"  # Dynamic destination prefix based on country_name

            # Upload the GIFs folder to GCS
            if os.path.exists(gif_folder):  # Ensure the folder exists
                upload_folder_to_gcs(bucket_name, gif_folder, destination_prefix)
                st.write(f"Uploaded GIFs from {gif_folder} to {bucket_name}/{destination_prefix}")
            else:
                st.error(f"GIF folder '{gif_folder}' does not exist.")


