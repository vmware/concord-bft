#!/usr/bin/env bash

# This script generates sets of key pairs - private/public keys to be used for
# concord client (participant node) transaction signing.
#
# Output is generated into output path output_base_path/transaction_signing_keys folder.
# If --output_base_path is not given by user, output is generated into output
# path script_working_path/transaction_signing_keys.
# Each generated set resides in a separate numeric sub-folder, range from 1 to
# num_participants.
#
# In general, it can be used by any 2 entities; the signer should use the
# transaction_signing_priv.pem to sign, and the verifier should use transaction_signing_pub.pem
# to verify the signature.

# This script assumes that the output path is empty. If not, it fails with a warning.
set -eo pipefail

usage() {
    printf "\n-----\nhelp:\n-----\n"
    printf "%s\n\n" "-h --help, print this message"
    printf "%s\n\n" "-n --num_participants <integer>, mandatory"
    printf "%s\n\n" "-r --private_key_name <string>, optional, default: transaction_signing_priv.pem"
    printf "%s\n\n" "-u --public_key_name <string>, optional, default: transaction_signing_pub.pem"
    printf "%s\n\t\t\t\t%s\n\t\t\t\t%s\n" "-o --output_base_path <string>, optional, base path in relative/absolute format" \
           "output is redirected to output_base_path/transaction_signing_keys folder" \
           "default: ./transaction_signing_keys"
}

parser() {
    num_participants=""
    output_base_path="./"
    output_folder="transaction_signing_keys"
    private_key_name="transaction_signing_priv.pem"
    public_key_name="transaction_signing_pub.pem"

    while [ $1 ]; do
        case $1 in
        -h | --help)
        usage
        exit
        ;;

        -n | --num_participants)
        num_participants=$2
        if [ ! $2 ]; then echo "error: bad input for option -n | --num_participants!" >&2; usage; exit; fi
        shift 2
        ;;

        -o | --output_base_path)
        output_base_path=$2
        if [ ! $2 ]; then echo "error: bad input for option -o | --output_base_path!" >&2; usage; exit; fi
        shift 2
        ;;

        -r | --private_key_name)
        private_key_name="$2"
        shift 2
        ;;

        -u | --public_key_name)
        public_key_name="$2"
        shift 2
        ;;

        *)
        echo "error: unknown input $1!" >&2
        usage
        exit
        ;;
        esac
    done

    # num_participants must be a number, and must be greater than zero
    if [[ -z ${num_participants} ]]; then
        echo "error: option -n | --num_participants is mandatory!" >&2
        usage
        exit 1
    fi

    re='^[0-9]+$'
    if ! [[ ${num_participants} =~ $re ]] ; then
        echo "error: option -n | --num_participants must be a number!" >&2
        usage
        exit 1
    fi

    if [ ! "$num_participants" -gt "0" ]; then
        echo "error: option -n | --num_participants must be a positive integer!" >&2
        usage
        exit;
    fi

    output_path=$(realpath ${output_base_path})
    if [ ! -d "${output_path}" ]; then
        echo "error: option -o | --output_base_path, path must exist!" >&2
        exit 1
    fi
    output_path=${output_path}/${output_folder}
    if [ -d "${output_path}" ]; then
        echo "error: --output_base_path ${output_path} already exists! Please delete it or supply a different path..."  >&2
        exit 1
    fi
}

parser $@

mkdir -p ${output_path}

for ((i=1; i<=${num_participants}; i++)); do
    current_path=${output_path}/${i}
    mkdir ${current_path}
    openssl genrsa -out ${current_path}/${private_key_name} 2048 > /dev/null
    openssl rsa -in ${current_path}/${private_key_name} \
                -pubout \
                -out ${current_path}/${public_key_name} > /dev/null
done

echo "Done, keys are under ${output_path}"


