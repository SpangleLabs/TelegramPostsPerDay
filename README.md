# Telegram to IRC log converter

Originally this project was supposed to be a simple tool to generate some statistics about telegram chats.
But then I decided that this problem is already solved by [pisg](http://pisg.sourceforge.net/), and wrote a tool to convert telegram chats to IRC (specifically irssi) style logs.

## Running it
1. Create a `config.json` file, like so:
    ```
    {
        "api_id": 000,
        "api_hash": "abcd1234"
    }
    ```
   - You will need to get your api ID and hash from https://my.telegram.org/apps
2. Run `python3 convert_to_irssi_logs.py`
   - From there, it will ask for your phone number to log you in, and you'll get a telegram auth code sent to you.
   - Then, on your first time running it, it will ask which chats you want to generate logs for.
   - (On future runs, you can pass the "skip" argument to skip these questions. i.e. `python3 convert_to_irssi_logs.py skip`)
3. Run `pisg` to generate the pretty output files, which will appear in the `pisg_output/` directory

Hints and tips:
- You can add your own pisg config to the pisg.cfg file.
- Extra user data can be added to `irclogs_cache/data_store.json` to the "user_extra_data" dictionary, with the key as the user ID, and then key=value for the things you would like to set in the generated users.cfg. This is useful for overriding real names that Telegram will serve up.
- It can be handy to add a symbolic link from pisg_output/ to some web directory.