import logging

from vox_harbor.big_bot.main import main

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)6s - %(name)s - %(message)s',
)

if __name__ == '__main__':
    main()
