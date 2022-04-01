#include <utt/Configuration.h>

#include <utt/TxIn.h>

std::ostream& operator<<(std::ostream& out, const libutt::TxIn& txin) {
    out << txin.coin_type << endl;
    out << txin.exp_date  << endl;
    out << txin.null      << endl;
    out << txin.vcm       << endl;
    out << txin.ccm       << endl;
    out << txin.coinsig   << endl;
    out << txin.pi               ;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::TxIn& txin) {
    in >> txin.coin_type; 
    libff::consume_OUTPUT_NEWLINE(in);
    in >> txin.exp_date ; 
    libff::consume_OUTPUT_NEWLINE(in);
    in >> txin.null     ; 
    libff::consume_OUTPUT_NEWLINE(in);
    in >> txin.vcm      ; 
    libff::consume_OUTPUT_NEWLINE(in);
    in >> txin.ccm      ; 
    libff::consume_OUTPUT_NEWLINE(in);
    in >> txin.coinsig  ; 
    libff::consume_OUTPUT_NEWLINE(in);
    in >> txin.pi       ; 
    return in;
}

namespace libutt {

} // end of namespace libutt
