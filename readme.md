# Simple resource tracker


Before you start:
- Check that you have Python >= 3.12 installed and configured in Path
- Create and activate virtual environment
- Install dependencies inside the virtual environment by running `pip install -r requirements.txt`
- Create a `sensors.conf` file in the `measure.py` directory and write the following:
`logs_location=absolute/path/to/openhardwaremonitor/end_path_with_the_location_of_the_go_file_without_date`  
example:
`logs_location=C:\Users\test_user\Desktop\openhardwaremonitor-v0.9.6\OpenHardwareMonitor\OpenHardwareMonitorLog`  

Open Issues:
- [ ] Charge difference not measured correctly
- [ ] Disk IO not used
- [ ] Html graph view not implemented
- [ ] Network IO not implemented
- [ ] Database Normalization on process and session charge difference pending
