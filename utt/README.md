# libutt

Steps:

 1. Fork
 2. Rename the repository to `libwhatever`
 3. Run `rename-library.sh whatever`
 4. Update this README file
 5. Enjoy developing your `libwhatever` library based on [libff](http://github.com/scipr-lab/libff/) and [libfqfft](http://github.com/scipr-lab/libfqfft)


## Build on Linux

Step zero is to clone this repo and `cd` to the right directory:

    cd <wherever-you-cloned-libutt>

If you're running OS X, make sure you have the Xcode **and** the Xcode developer tools installed:

    xcode-select --install

First, install deps using:

    scripts/linux/install-libs.sh
    scripts/linux/install-deps.sh

Then, set up the environment. This **will store the built code in ~/builds/utt/**:

    . scripts/linux/set-env.sh release

...you can also use `debug`, `relwithdebug` or `trace` as an argument for `set-env.sh`.

To build:

    make.sh

..tests, benchmarks and any other binaries are automatically added to `PATH` but can also be found in

    cd ~/builds/utt/master/release/libutt/bin/

(or replace `release` with `debug` or whatever you used in `set-env.sh`)

## Useful scripts

There's a bunch of useful scripts in `scripts/linux/`:

 - `cols.sh` for viewing CSV data in the terminal
 - `generate-qsbdh-params.sh` for generating $q$-SDH public parameters (e.g., for the Kate-Zaveruch-Goldberg polynomial commitments)

## Git submodules

This is just for reference purposes. 
No need to execute these.
To fetch the submodules, just do:

    git submodule init
    git submodule update

For historical purposes, (i.e., don't execute these), when I set up the submodules, I did:
    
    cd depends/
    git submodule add git://github.com/herumi/ate-pairing.git
    git submodule add git://github.com/herumi/xbyak.git
    git submodule add git://github.com/scipr-lab/libff.git
    git submodule add https://github.com/alinush/libfqfft.git

To update your submodules with changes from their upstream github repos, do:

    git submodule foreach git pull origin master

