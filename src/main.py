import logging
import argparse
from detect_transfer import DDLTransferManager

logging.basicConfig(filename='ddl_migration.log', filemode='w', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H:%M:%S")
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

def main():
    parser = argparse.ArgumentParser(description='Enter Source with -s or --source & Destination with -d or --destination for DDL migration-conversion')
    parser.add_argument('-s', '--source', type=str, nargs='+', help="List of source elements")
    parser.add_argument('-d', '--destination', type=str, nargs='+', help="List of destination elements")
    parser.add_argument('-i', '--interval', type=int, default=10, help="Check interval in seconds")
    args = parser.parse_args()
    
    source_list = args.source if args.source else []
    destination_list = args.destination if args.destination else []
    check_interval = args.interval

    config_file = "config/db.ini"

    transfer_manager = DDLTransferManager(source_list, destination_list, config_file)

    try:
        transfer_manager.run(check_interval)
    except KeyboardInterrupt:
        print("DDL migration-conversion tool stopped.")
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
