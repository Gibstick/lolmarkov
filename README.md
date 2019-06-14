# Discord Scraper and Markov Chain Talkbot

This is a super simple duo of bots: one for scraping the entire history of a
server, and another for creating a bot to generate messages from users using
markov chains. The talkbot uses the sqlite3 database created by the scrape bot
to create its markov chains.

# Dependencies

See [`requirements.txt`](requirements.txt).

# Running

Each bot needs its own token. Place your tokens in their respective config
files, `config.ini` and `config_lolmarkov.ini`. Pass `--help` to see more
command-line options.

# Permissions

The scraper needs history permissions (`permissions=65536`) and the markov
chain talkbot needs "Send Messages" and "Add Reactions" (`permissions=2112`).

# Commands

The scraping bot works in the background and has no commands.

The talkbot has the following commands.

- `$switch USER`: Switch the model to the user. Users currently in the server
  can be specified by ID, Mention, Username, or Username#discriminator. Users
  not in the server can be specifeid by Username#discrimnator as they appeared
  when the data set was scraped.
- `$talk [init]`: Generate a random message, given up to two words to start the
  sentence. This command may fail if there is not enough data for the current
  model.
