from glob import glob
import shutil
import pandas as pd

parent_directory = r"C:\Users\Nigel\Documents\FOREX\SILVER"
raw = [
    r"C:\Users\Nigel\Documents\FOREX\BRONZE\FOREXDATA\*\*\*",
    r"C:\Users\Nigel\Documents\FOREX\BRONZE\FOREXDATA\*\*",
]


for paths in raw:
    for i, filename_with_path in enumerate(glob(paths, recursive=True)):
        filename = filename_with_path.split("\\")[-1]
        file_extension = filename.split(".")[-1]
        new_filename = filename.split(".")[0] + "_" + str(i)

        if "eurusd" in filename.lower():
            destination_path = rf"{parent_directory}\EURUSD"
        elif "usdjpy" in filename.lower():
            destination_path = rf"{parent_directory}\USDJPY"
        elif "audusd" in filename.lower():
            destination_path = rf"{parent_directory}\AUDUSD"
        elif "gbpusd" in filename.lower():
            destination_path = rf"{parent_directory}\GBPUSD"
        elif "usdcad" in filename.lower():
            destination_path = rf"{parent_directory}\USDCAD"
        else:
            destination_path = parent_directory
            continue

        # Convert to CSV
        if file_extension == "xlsx":
            pd.read_excel(filename_with_path).to_csv(
                rf"{destination_path}\{new_filename}.csv", index=None, header=True
            )
        # Copy
        elif file_extension == "csv":
            shutil.copyfile(
                filename_with_path, rf"{destination_path}\{new_filename}.csv"
            )
        else:
            raise ValueError("File Extension Not Recognized!")
