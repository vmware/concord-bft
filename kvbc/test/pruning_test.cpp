// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include <bftengine/ControlStateManager.hpp>
#include <bftengine/ControlStateManager.hpp>
#include "NullStateTransfer.hpp"
#include "categorization/kv_blockchain.h"
#include "categorization/updates.h"
#include "db_interfaces.h"
#include "endianness.hpp"
#include "pruning_handler.hpp"

#include "storage/test/storage_test_common.h"

#include <cassert>
#include <cstdint>
#include <exception>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using concord::storage::rocksdb::NativeClient;
using concord::kvbc::BlockId;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::pruning;
namespace {
const NodeIdType replica_0 = 0;
const NodeIdType replica_1 = 1;
const NodeIdType replica_2 = 2;
const NodeIdType replica_3 = 3;
std::string privateKey_0 =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100C55B8F7979BF24B335017082BF33EE2960E3"
    "A0"
    "68DCDB45CA3017214BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B2347"
    "48"
    "8B0FA3F99B05CFFBB45F80960669594B58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE5"
    "0B"
    "EF374FCDCE1C8CEBCA1EF12AF492402A6F56DC9338834162F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B484F"
    "72"
    "A2BC8C89F86DB52ECC40763B6298879DE686C9A2A78604A503609BA34B779C4F55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF88292D"
    "4C"
    "01F99EFE3CC912D09B020111028201001D05EF73BF149474B4F8AEA9D0D2EE5161126A69C6203EF8162184E586D4967833E1F9BF56C89F68"
    "AD"
    "35D54D99D8DB4B7BB06C4EFD95E840BBD30C3FD7A5E890CEF6DB99E284576EEED07B6C8CEBB63B4B80DAD2311D1A706A5AC95DE768F01721"
    "3B"
    "896B9EE38D2E3C2CFCE5BDF51ABD27391761245CDB3DCB686F05EA2FF654FA91F89DA699F14ACFA7F0D8030F74DBFEC28D55C902A27E9C03"
    "AB"
    "1CA2770EFC5BE541560D86FA376B1A688D92124496BB3E7A3B78A86EBF1B694683CDB32BC49431990A18B570C104E47AC6B0DE5616851F43"
    "09"
    "CFE7D0E20B17C154A3D85F33C7791451FFF73BFC4CDC8C16387D184F42AD2A31FCF545C3F9A498FAAC6E94E902818100F40CF9152ED4854E"
    "1B"
    "BF67C5EA185C52EBEA0C11875563AEE95037C2E61C8D988DDF71588A0B45C23979C5FBFD2C45F9416775E0A644CAD46792296FDC68A98148"
    "F7"
    "BD3164D9A5E0D6A0C2DF0141D82D610D56CB7C53F3C674771ED9ED77C0B5BF3C936498218176DC9933F1215BC831E0D41285611F512F6832"
    "7E"
    "4FBD9E5C5902818100CF05519FD69D7C6B61324F0A201574C647792B80E5D4D56A51CF5988927A1D54DF9AE4EA656AE25961923A0EC046F1"
    "C5"
    "69BAB53A64EB0E9F5AB2ABF1C9146935BA40F75E0EB68E0BE4BC29A5A0742B59DF5A55AB028F1CCC42243D2AEE4B74344CA33E72879EF2D1"
    "CD"
    "D874A7F237202AC7EB57AEDCBD539DEFDA094476EAE613028180396C76D7CEC897D624A581D43714CA6DDD2802D6F2AAAE0B09B885974533"
    "E5"
    "14D6167505C620C51EA41CA70E1D73D43AA5FA39DA81799922EB3173296109914B98B2C31AAE515434E734E28ED31E8D37DA99BA11C2E693"
    "B6"
    "398570ABBF6778A33C0E40CC6007E23A15C9B1DE6233B6A25304B91053166D7490FCD26D1D8EAC5102818079C6E4B86020674E392CA6F6E5"
    "B2"
    "44B0DEBFBF3CC36E232F7B6AE95F6538C5F5B0B57798F05CFD9DFD28D6DB8029BB6511046A9AD1F3AE3F9EC37433DFB1A74CC7E9FAEC08A7"
    "9E"
    "D9D1D8187F8B8FA107B08F7DAFE3633E1DCC8DC9A0C8689EB55A41E87F9B12347B6A06DB359D89D6AFC0E4CA2A9FF6E5E46EF8BA2845F396"
    "65"
    "0281802A89B2BD4A665A0F07DCAFA6D9DB7669B1D1276FC3365173A53F0E0D5F9CB9C3E08E68503C62EA73EB8E0DA42CCF6B136BF4A85B0A"
    "C4"
    "24730B4F3CAD8C31D34DD75EF2A39B6BCFE3985CCECC470CF479CF0E9B9D6C7CE1C6C70D853728925326A22352DF73B502D4D3CBC2A770DE"
    "27"
    "6E1C5953DF7A9614C970C94D194CAE9188";
std::string privateKey_1 =
    "308204BC020100300D06092A864886F70D0101010500048204A6308204A20201000282010100CAF26C09B1F6F056F66B54AA0EA002EA2361"
    "51"
    "3A721A58E44472ADE74DBD833890ED9EE8E0CFF1B84C3DFE9E6226C8CEBFFEDFE71A6BCFE5A599D02FD9940997E864D400BF440BA7B1A568"
    "A5"
    "ACF7688B025B5B207E5651E597141FAEF733FA9D87732B8FBA31E082BE777AEB992B00873764655C3B1F8FBF8DD2446F4F9F79883E21DBC1"
    "A1"
    "7431E42AF01B9D4DE54B9880E7FB397655C79D7C3A5981CA64A32E2F05FCF0B127F4C6D349056276992B7403D36E03D14C45B6F5F6678628"
    "0C"
    "921236A17EE0F311821BDF925A47DCA590BB31C60646692014317BE0DD32248DF557A77F80F336C82461B659266F74F495BA5CE194043505"
    "B5"
    "F0C363263BF73BB897020111028201005F8123C853BF8028EC6EBE6E2500015F1FB55366CC48A24D4D6324A915865BDE6251B4315ABC3583"
    "E7"
    "A4B40E4C4E7C9D8786FFF448AB34A84DEE079E0C096DED221154B50EB69C12ADF37C8A334740416A85580F4A82F95CFBCD3C1619FA57D1A9"
    "27"
    "238EEE3596D41D656705754169A90B021194D08752B47EF9899DCB1DDED5DCDDA688D9FA88F9303E99CAB040D2E47DCE866717580C2CD01D"
    "B9"
    "4E8FE215EAA254432E3B399B90260255DEE7FADED4643814C0DE41DE237BBB7173CFF16AC9237AADA4595FD95EB92E30D2127C7761C6A1CD"
    "75"
    "C1F12F5B92B6602E1CDC06E112321B0A41C8A102D785B7C4C7D2441DA983346B81873984DD680FB4B1C9AD7102818100D5E41BE52CECFB74"
    "05"
    "01B87C631DEFB3DD855EE95553A816BE8D3DFAE200086E1C12E9FBDDD8737F2CCE2CED1D82431ADC63C1B34EA59C27E98283EB20EAF8F5A2"
    "9D"
    "45ACE2E5C3173F396DF4356D54F3ED0CF9BBA222E3B9194CD15F88B6AB8FAFDFACACCC97D87555CD1E864D09DF795FB8BE3F9BE2CB8E52F6"
    "41"
    "FCEDC8E97902818100F2E6BDF9A552D35E9F695C52343D9BBF180BBEB50F6705A7836DF1BFF6A42C2D7A000432957516B555B5E1FBAC21CE"
    "D5"
    "D2788036AA5AB183A5859284ED409631289F8836D240111B56D6C4953FEFBE177EA137F08ADCABD5CAD07F709E83BB29B0F55AD09E65F5C6"
    "56"
    "8FE166FF4BE581F4F2066025E3902819EFC2DF0FA63E8F0281803EE8BCE90D36A44F4CC44551C2CC91CB7D637644A0A022610ADE3F67E81E"
    "20"
    "98DB149F2BF5F45E347696FE279F446E16F586C080081297570871AE5436DBB2A2993D50BA60DA2A5221A77AB13CE3EBCF45B885AFA82861"
    "18"
    "52BC3D94919F23667F058D23C3B4309AFB1E36278011F66EFE0928E58833A547FA486DC2DC8662C902818100AB75B346CF0D49E870869B85"
    "52"
    "0D5EE13E26687FCEA3130CD53E8C8780EC5B6B652D3023B4CB1F1696DABDA2979F64D32B27E208784004D565C7B2B82F006A0495255117A3"
    "78"
    "848BC4D3D60EFFF4862EB3BD186D8F325B2D801AB44F7EF3932C7CE96D47F75707D74C2953D03BBD1A79DA1440BC56FAFC588AC75C613839"
    "1D"
    "1902818100BBC05F561AEF4BDE1ECABB5CD313E6173F5E46AC85B4462A8CA689E4E84B95E66733081518DB01714B7B18E44B780DB5A6DA26"
    "12"
    "506F46E4D91C8CB582AFD54C7509C9947EC754BFDD0D35FAECE2E729DBD21A62E33C3DEF556913ECF4C8D75910E860D36AFD0977FF9114AC"
    "EA"
    "8F44882259C6F1D8314B653F64F4655CFD68A6";
std::string privateKey_2 =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100DA68B2830103028D0ECD911D20E732257F8A"
    "39"
    "BC5214B670C8F1F836CD1D88E6421ABD3C47F16E37E07DCFE89FC11ECBED73896FC978B7E1519F59318F3E24BB5C8962B0EA44889C3A8102"
    "1C"
    "C9E8EAA94099B9305B0F7731C27CC528CFEE710066CB69229F8AA04DBD842FA8C3143CB0EF1164E45E34B114FBA610DBDFC7453DAD995743"
    "AD"
    "1489AA131D0C730163ECA7317048C1AD1008ED31A32CB773D94B5317CAAF95674582DCA1AC9FEBE02079836B52E1E3C8D39571018A765765"
    "0D"
    "52D2DF3363F88A690628D64DCBECB88BBA432F27C32E7BC5BC0F1D16D0A027D1D40362F8CEDBE8BED05C2BCE96F33A75CD70014626B32E5E"
    "E8"
    "1EBD3116F6F1E5231B02011102820100201E749ACB716241EB96B375398B6941BFEEAE23393F480186F668444B572AB873220CC519A38126"
    "55"
    "B8261AAE14DEE1C1097617F7FB2A199B0FE7783AB650B22432524731828C8F7203E9B8F08422824D43C868FE55190ED8D61CFE78EE5BE978"
    "87"
    "5339CC2AF974D81AF7F32BBF361A050A165DD19E5646D9B68A02377F2FD417BE92D2722CEE33B3A0F7B111639C092B4024BB08ACEBE9F9BC"
    "28"
    "C14BB611DBE627136B568493C6995D81442F09DFEFBA4378A34DEF28F3BD862F4C28BECA87A95551FC2DB48766F82752C7E8B34E85995901"
    "90"
    "B12A648AC1E361FE28A3253EC615381A066BFD351C18F0B236429A950106DD4F1134A3DE6CEB1094B7CE5C7102818100EA51580294BB0FD7"
    "DA"
    "FDC00364A9B3C4781F70C4BBE8BDE0D82895ADE549E25388E78A53EF83CF9A3D9528F70CB5A57A9A187D5AC68682F03B26C2F6A409E1554F"
    "ED"
    "061BDC944985102F2988A5A8CD639BA7890FD689D7171612159AAA818F5502EF80D5D13339826A98914A321B5B4512EC24AF3EE80F982D34"
    "18"
    "0C85B3E38102818100EE9E7F43A98E593F72A584EEC014E0A46003116B82F5D3A21DAE4EB3F21FBC5B71D96E012B6F5FFBEACED4BEC6C147"
    "AA"
    "AB6F9698F0592F3A8A6CD827ABF210497635639043D5F0B461E00914B152D6ECB3EFC9138A1B9FAEE09420AB9C2E1436B6AC36EEB8AD4370"
    "9B"
    "BFA0ED30FBF09C1A91BAB71410E49E11BE8E2A571C649B02818044EABF8849DCAA4E8BB40B4C4AC8802AB9EB212ACDDB0AAB8ADEC29C8EBB"
    "60"
    "AF284419A0376300D3030DC0C121DB128D789DCA841C45AE0A6BC01B397B8A6F7371DC4D1740E051DBD79566919A2296C2F18BA0C86C46A8"
    "AC"
    "6FE73387D7CBC0BEA682AD6C105A5C356AA557E8A553571450DC0ACA218F8C1DB2F1343FEB16CA71028180704A963DF57029FFBD7B11614B"
    "55"
    "1E6B7879EA1479DD184C4A33E8CD26A585D0AE0BF7881470A5A3B9CABE77E50FA941419DEC8434DEACD04124297C14AE25C837A0A752F2BF"
    "07"
    "DC6A4B4F91446337F6EB43A9EB13D0C39D96DC4B9C0D42DC55FB9C5615FC8DC5622B2D006F9E94AD76A31766ECBE26113B53A4F79B744998"
    "C1"
    "028180124E2A5DAC7AC7017647FE1B6871484B7B38A3C060B992B871939C8A75BC35DB771F041910C0092E01B545E1910109AA827F34F563"
    "27"
    "15E06819ADED60117FD8B9400C3F307EA022D5AC5394E65D28EF434DC068494D33ACF70B43791C1C13C35AC680E0038DB411ADEBBC067F47"
    "7E"
    "01E8CF793FB076AE47BD83B935B8041DC1";
std::string privateKey_3 =
    "308204BB020100300D06092A864886F70D0101010500048204A5308204A10201000282010100B4D984129ECC2F4350596DD602EB5337B78E"
    "33"
    "C4D945C70C2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F2"
    "02"
    "E474AF535E92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD18"
    "5E"
    "1CF3E78B6B82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF"
    "4B"
    "C381D21F9ABF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7"
    "C9"
    "AA1224D190B2D8A83B020111028201004FC95FEA18E19C6176459256E32B95A7A3CDCB8B8D08322B04A6ACE790BDC5BC806C087D19F2782D"
    "DB"
    "0B444C0BC6710ED9564E8073061A66F0D163F5E59D8DB764C3C8ECC523BD758B13815BA1064D597C8C078AF7A450241FED30DDAFEF2CF4FC"
    "48"
    "ABD336469D648B4DB70C1A2AF86D9BDC56AC6546C882BD763A963329844BF0E8C79E5E7A5C227A1CDB8473965090084BEAC728E4F86670BA"
    "E0"
    "5DA589FAE1EBC98A26BF207261EECE5A92759DE516306B0B2F715370586EF45447F82CC37206F70D762AAB75215482A67FE6742E9D644AB7"
    "65"
    "4F02476EAE15A742FCB2266DAE437DC76FB17E1698D4987945C779A4B89F5FB3F5E1B194EFA4024BA3797F2502818100E2BDA6F0031EAA02"
    "7B"
    "1E8099D9D2CF408B8C9D83A22BFBAAD3BCFA952469A001C25E5E2E89DAF28636633E8D7C5E1E5FD8384767DB5B384DAB38147EA46871D5E3"
    "1E"
    "DC110026E42E90C44F02110A369ACF0445C617CC25CD1207F116B9AF2FA78A89E6C6345EAFD0607CB145410DD6619AC0C034B54283A0065B"
    "93"
    "3550B8DACF02818100CC2FDB49EB3E45DB2EB644048C3C35E3BB50980453DB8EB54DD5595CA2DBC43A2F2912D0F696E6126AFBF5D7777BAB"
    "B2"
    "6AC931030B8896343BC19EA30B8EEBFECE2617A2564B3D66E29DF66708254877CC33E699501A2BA4D0D7D02F068B1DCF6C7A0794F24BEE83"
    "BE"
    "2E8453D3E436C3B59D2D9BEEB5B38541EF170544EA46D502818100AD63DA02D5359110F4BCF8EE1F0A9E7CA6F30F0A4ED6570A29726544DF"
    "9C"
    "10F2495738F6696B31EE29972FD59B57082B2CDFBE223E54D0B3DD49009D144FDE94808102A396B454239BE169982B25ED85712162886C8D"
    "0D"
    "D90DC9D67ACA3AABF8971E28F1EBCFEFDB95140F16D764EF3B947547AFD5E791D4B9915274108D5C07028180300B42A7FB1DB61574671F10"
    "20"
    "FF1BBD1D03E7888C33A91B99D7D8CA80AC2E2BCEDC7CE5DFAB08F546596705858682C09198C03CF3A7AADF1D1E7FADE49A196921725FE9F6"
    "2F"
    "D236537076365C4501FE11EE182412D8FB35D6C95E292EB7524EEC58F2B9A26C381EFF92797D22CC491EFD8E6515A1942A3D78ECF65B97BE"
    "A7"
    "410281806625E51F70BB9E00D0260941C56B22CDCECC7EA6DBC2C28A1CDA0E1AFAACD39C5E962E40FCFC0F5BBA57A7291A1BDC5D3D29495B"
    "45"
    "CE2B3BEC7748E6C351FF934537BCA246CA36B840CF68BE6D473F7EE079B0EFDFF6B1BB554B06093E18B269FC3BD3F9876376CA7C673A93F1"
    "42"
    "7088BF0990AB8E232F269B5DBCD446385A66";
std::string privateKey_4 =
    "308204BB020100300D06092A864886F70D0101010500048204A5308204A10201000282010100B4D984129ECC2F4350596DD602EB5337B78E"
    "33"
    "C4D945C70C2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F2"
    "02"
    "E474AF535E92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD18"
    "5E"
    "1CF3E78B6B82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF"
    "4B"
    "C381D21F9ABF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7"
    "C9"
    "AA1224D190B2D8A83B020111028201004FC95FEA18E19C6176459256E32B95A7A3CDCB8B8D08322B04A6ACE790BDC5BC806C087D19F2782D"
    "DB"
    "0B444C0BC6710ED9564E8073061A66F0D163F5E59D8DB764C3C8ECC523BD758B13815BA1064D597C8C078AF7A450241FED30DDAFEF2CF4FC"
    "48"
    "ABD336469D648B4DB70C1A2AF86D9BDC56AC6546C882BD763A963329844BF0E8C79E5E7A5C227A1CDB8473965090084BEAC728E4F86670BA"
    "E0"
    "5DA589FAE1EBC98A26BF207261EECE5A92759DE516306B0B2F715370586EF45447F82CC37206F70D762AAB75215482A67FE6742E9D644AB7"
    "65"
    "4F02476EAE15A742FCB2266DAE437DC76FB17E1698D4987945C779A4B89F5FB3F5E1B194EFA4024BA3797F2502818100E2BDA6F0031EAA02"
    "7B"
    "1E8099D9D2CF408B8C9D83A22BFBAAD3BCFA952469A001C25E5E2E89DAF28636633E8D7C5E1E5FD8384767DB5B384DAB38147EA46871D5E3"
    "1E"
    "DC110026E42E90C44F02110A369ACF0445C617CC25CD1207F116B9AF2FA78A89E6C6345EAFD0607CB145410DD6619AC0C034B54283A0065B"
    "93"
    "3550B8DACF02818100CC2FDB49EB3E45DB2EB644048C3C35E3BB50980453DB8EB54DD5595CA2DBC43A2F2912D0F696E6126AFBF5D7777BAB"
    "B2"
    "6AC931030B8896343BC19EA30B8EEBFECE2617A2564B3D66E29DF66708254877CC33E699501A2BA4D0D7D02F068B1DCF6C7A0794F24BEE83"
    "BE"
    "2E8453D3E436C3B59D2D9BEEB5B38541EF170544EA46D502818100AD63DA02D5359110F4BCF8EE1F0A9E7CA6F30F0A4ED6570A29726544DF"
    "9C"
    "10F2495738F6696B31EE29972FD59B57082B2CDFBE223E54D0B3DD49009D144FDE94808102A396B454239BE169982B25ED85712162886C8D"
    "0D"
    "D90DC9D67ACA3AABF8971E28F1EBCFEFDB95140F16D764EF3B947547AFD5E791D4B9915274108D5C07028180300B42A7FB1DB61574671F10"
    "20"
    "FF1BBD1D03E7888C33A91B99D7D8CA80AC2E2BCEDC7CE5DFAB08F546596705858682C09198C03CF3A7AADF1D1E7FADE49A196921725FE9F6"
    "2F"
    "D236537076365C4501FE11EE182412D8FB35D6C95E292EB7524EEC58F2B9A26C381EFF92797D22CC491EFD8E6515A1942A3D78ECF65B97BE"
    "A7"
    "410281806625E51F70BB9E00D0260941C56B22CDCECC7EA6DBC2C28A1CDA0E1AFAACD39C5E962E40FCFC0F5BBA57A7291A1BDC5D3D29495B"
    "45"
    "CE2B3BEC7748E6C351FF934537BCA246CA36B840CF68BE6D473F7EE079B0EFDFF6B1BB554B06093E18B269FC3BD3F9876376CA7C673A93F1"
    "42"
    "7088BF0990AB8E232F269B5DBCD446385A66";
std::string publicKey_0 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100C55B8F7979BF24B335017082BF33EE2960E3A068DCDB45"
    "CA"
    "3017214BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B2347488B0FA3F9"
    "9B"
    "05CFFBB45F80960669594B58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE50BEF374FCD"
    "CE"
    "1C8CEBCA1EF12AF492402A6F56DC9338834162F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B484F72A2BC8C89"
    "F8"
    "6DB52ECC40763B6298879DE686C9A2A78604A503609BA34B779C4F55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF88292D4C01F99EFE"
    "3C"
    "C912D09B020111";
std::string publicKey_1 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100CAF26C09B1F6F056F66B54AA0EA002EA2361513A721A58"
    "E4"
    "4472ADE74DBD833890ED9EE8E0CFF1B84C3DFE9E6226C8CEBFFEDFE71A6BCFE5A599D02FD9940997E864D400BF440BA7B1A568A5ACF7688B"
    "02"
    "5B5B207E5651E597141FAEF733FA9D87732B8FBA31E082BE777AEB992B00873764655C3B1F8FBF8DD2446F4F9F79883E21DBC1A17431E42A"
    "F0"
    "1B9D4DE54B9880E7FB397655C79D7C3A5981CA64A32E2F05FCF0B127F4C6D349056276992B7403D36E03D14C45B6F5F66786280C921236A1"
    "7E"
    "E0F311821BDF925A47DCA590BB31C60646692014317BE0DD32248DF557A77F80F336C82461B659266F74F495BA5CE194043505B5F0C36326"
    "3B"
    "F73BB897020111";
std::string publicKey_2 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100DA68B2830103028D0ECD911D20E732257F8A39BC5214B6"
    "70"
    "C8F1F836CD1D88E6421ABD3C47F16E37E07DCFE89FC11ECBED73896FC978B7E1519F59318F3E24BB5C8962B0EA44889C3A81021CC9E8EAA9"
    "40"
    "99B9305B0F7731C27CC528CFEE710066CB69229F8AA04DBD842FA8C3143CB0EF1164E45E34B114FBA610DBDFC7453DAD995743AD1489AA13"
    "1D"
    "0C730163ECA7317048C1AD1008ED31A32CB773D94B5317CAAF95674582DCA1AC9FEBE02079836B52E1E3C8D39571018A7657650D52D2DF33"
    "63"
    "F88A690628D64DCBECB88BBA432F27C32E7BC5BC0F1D16D0A027D1D40362F8CEDBE8BED05C2BCE96F33A75CD70014626B32E5EE81EBD3116"
    "F6"
    "F1E5231B020111";
std::string publicKey_3 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100B4D984129ECC2F4350596DD602EB5337B78E33C4D945C7"
    "0C"
    "2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F202E474AF53"
    "5E"
    "92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD185E1CF3E78B"
    "6B"
    "82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF4BC381D21F"
    "9A"
    "BF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7C9AA1224D1"
    "90"
    "B2D8A83B020111";
std::string publicKey_4 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100B4D984129ECC2F4350596DD602EB5337B78E33C4D945C7"
    "0C"
    "2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F202E474AF53"
    "5E"
    "92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD185E1CF3E78B"
    "6B"
    "82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF4BC381D21F"
    "9A"
    "BF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7C9AA1224D1"
    "90"
    "B2D8A83B020111";

bftEngine::ReplicaConfig &replicaConfig = bftEngine::ReplicaConfig::instance();
std::map<uint64_t, std::string> private_keys_of_replicas;
void setUpKeysConfiguration_4() {
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_0, publicKey_0));
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_1, publicKey_1));
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_2, publicKey_2));
  replicaConfig.publicKeysOfReplicas.insert(std::pair<uint16_t, const std::string>(replica_3, publicKey_3));
  private_keys_of_replicas[replica_0] = privateKey_0;
  private_keys_of_replicas[replica_1] = privateKey_1;
  private_keys_of_replicas[replica_2] = privateKey_2;
  private_keys_of_replicas[replica_3] = privateKey_3;
}

class test_rocksdb : public ::testing::Test {
  void SetUp() override {
    destroyDb();
    db = TestRocksDb::createNative();
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    db.reset();
    ASSERT_EQ(0, db.use_count());
    cleanup();
  }

 protected:
  std::shared_ptr<NativeClient> db;
};

const auto GENESIS_BLOCK_ID = BlockId{1};
const auto LAST_BLOCK_ID = BlockId{150};
const auto REPLICA_PRINCIPAL_ID_START = 0;
const auto CLIENT_PRINCIPAL_ID_START = 20000;
const std::uint32_t TICK_PERIOD_SECONDS = 1;
const std::uint64_t BATCH_BLOCKS_NUM = 60;

class TestStorage : public IReader, public IBlockAdder, public IBlocksDeleter {
 public:
  TestStorage(std::shared_ptr<::concord::storage::rocksdb::NativeClient> native_client)
      : bc_{native_client,
            false,
            std::map<std::string, CATEGORY_TYPE>{{kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}} {}

  // IBlockAdder interface
  BlockId add(categorization::Updates &&updates) override { return bc_.addBlock(std::move(updates)); }

  // IReader interface
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override {
    return bc_.get(category_id, key, block_id);
  }

  std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                 const std::string &key) const override {
    return bc_.getLatest(category_id, key);
  }

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override {
    bc_.multiGet(category_id, keys, versions, values);
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override {
    bc_.multiGetLatest(category_id, keys, values);
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override {
    bc_.multiGetLatestVersion(category_id, keys, versions);
  }

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override {
    return bc_.getLatestVersion(category_id, key);
  }

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override {
    return bc_.getBlockUpdates(block_id);
  }

  BlockId getGenesisBlockId() const override {
    if (mockGenesisBlockId.has_value()) return mockGenesisBlockId.value();
    return bc_.getGenesisBlockId();
  }

  BlockId getLastBlockId() const override { return bc_.getLastReachableBlockId(); }

  // IBlocksDeleter interface
  void deleteGenesisBlock() override {
    const auto genesisBlock = bc_.getGenesisBlockId();
    bc_.deleteBlock(genesisBlock);
  }

  BlockId deleteBlocksUntil(BlockId until) override {
    const auto genesisBlock = bc_.getGenesisBlockId();
    if (genesisBlock == 0) {
      throw std::logic_error{"Cannot delete a block range from an empty blockchain"};
    } else if (until <= genesisBlock) {
      throw std::invalid_argument{"Invalid 'until' value passed to deleteBlocksUntil()"};
    }

    const auto lastReachableBlock = bc_.getLastReachableBlockId();
    const auto lastDeletedBlock = std::min(lastReachableBlock, until - 1);
    for (auto i = genesisBlock; i <= lastDeletedBlock; ++i) {
      ConcordAssert(bc_.deleteBlock(i));
    }
    return lastDeletedBlock;
  }

  void setGenesisBlockId(BlockId bid) { mockGenesisBlockId = bid; }

 private:
  KeyValueBlockchain bc_;
  std::optional<BlockId> mockGenesisBlockId = {};
};

class TestStateTransfer : public bftEngine::impl::NullStateTransfer {
 public:
  void addOnTransferringCompleteCallback(std::function<void(uint64_t)> callback,
                                         StateTransferCallBacksPriorities priority) override {
    callback_ = callback;
  }

  void complete() { callback_(0); }

 private:
  std::function<void(uint64_t)> callback_;
};

using ReplicaIDs = std::set<std::uint64_t>;
TestStateTransfer state_transfer;

void CheckLatestPrunableResp(const concord::messages::LatestPrunableBlock &latest_prunable_resp,
                             int replica_idx,
                             const RSAPruningVerifier &verifier) {
  ASSERT_EQ(latest_prunable_resp.replica, replica_idx);
  ASSERT_TRUE(verifier.verify(latest_prunable_resp));
}

// This blockchain contains only versioned keys. TODO: add more categories

void InitBlockchainStorage(std::size_t replica_count,
                           TestStorage &s,
                           bool offset_ts = false,
                           bool empty_blockchain = false,
                           bool increment_now = false) {
  if (empty_blockchain) {
    ASSERT_EQ(s.getLastBlockId(), 0);
    return;
  }
  for (auto i = GENESIS_BLOCK_ID; i <= LAST_BLOCK_ID; ++i) {
    // To test the very basic functionality of pruning, we will override a single key over and over again.
    // Every previous versions should be deleted on pruning.
    concord::kvbc::categorization::VersionedUpdates versioned_updates;
    versioned_updates.addUpdate(std::string({0x20}), std::to_string(i));
    versioned_updates.calculateRootHash(false);

    concord::kvbc::categorization::Updates updates;
    updates.add(kConcordInternalCategoryId, std::move(versioned_updates));

    BlockId blockId;
    blockId = s.add(std::move(updates));

    // Make sure we've inserted the correct block ID.
    ASSERT_EQ(i, blockId);

    // Make sure our storage mock works properly.
    {
      auto stored_val = s.get(kConcordInternalCategoryId, std::string({0x20}), blockId);
      ASSERT_TRUE(stored_val.has_value());
      VersionedValue out = std::get<VersionedValue>(stored_val.value());
      ASSERT_EQ(blockId, out.block_id);
      ASSERT_EQ(std::to_string(i), out.data);
    }
  }

  // Make sure getLastBlock() works properly and it is equal to
  // LAST_BLOCK_ID.
  ASSERT_EQ(s.getLastBlockId(), LAST_BLOCK_ID);
}

concord::messages::PruneRequest ConstructPruneRequest(std::size_t client_idx,
                                                      const std::map<uint64_t, std::string> &private_keys,
                                                      BlockId min_prunable_block_id = LAST_BLOCK_ID,
                                                      std::uint32_t tick_period_seconds = TICK_PERIOD_SECONDS,
                                                      std::uint64_t batch_blocks_num = BATCH_BLOCKS_NUM) {
  concord::messages::PruneRequest prune_req;
  prune_req.sender = client_idx;
  uint64_t i = 0u;
  for (auto &[idx, pkey] : private_keys) {
    auto &latest_block = prune_req.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
    latest_block.replica = idx;
    // Send different block IDs.
    latest_block.block_id = min_prunable_block_id + i;

    auto block_signer = RSAPruningSigner{pkey};
    block_signer.sign(latest_block);
    i++;
  }
  prune_req.tick_period_seconds = tick_period_seconds;
  prune_req.batch_blocks_num = batch_blocks_num;
  return prune_req;
}

TEST_F(test_rocksdb, sign_verify_correct) {
  const auto replica_count = 4;
  uint64_t sending_id = 0;
  uint64_t client_proxy_count = 4;
  const auto verifier = RSAPruningVerifier{replicaConfig.publicKeysOfReplicas};
  std::vector<RSAPruningSigner> signers;
  signers.reserve(replica_count);
  for (auto i = 0; i < replica_count; ++i) {
    signers.emplace_back(RSAPruningSigner{private_keys_of_replicas[i]});
  }

  // Sign and verify a LatestPrunableBlock message.
  {
    concord::messages::LatestPrunableBlock block;
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    block.block_id = LAST_BLOCK_ID;
    signers[sending_id].sign(block);

    ASSERT_TRUE(verifier.verify(block));
  }

  // Sign and verify a PruneRequest message.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }
    ASSERT_TRUE(verifier.verify(request));
  }
}

TEST_F(test_rocksdb, verify_malformed_messages) {
  const auto replica_count = 4;
  const auto client_proxy_count = replica_count;
  const auto sending_id = 1;
  const auto verifier = RSAPruningVerifier{replicaConfig.publicKeysOfReplicas};
  std::vector<RSAPruningSigner> signers;
  signers.reserve(replica_count);
  for (auto i = 0; i < replica_count; ++i) {
    signers.emplace_back(RSAPruningSigner{private_keys_of_replicas[i]});
  }

  // Break verification of LatestPrunableBlock messages.
  {
    concord::messages::LatestPrunableBlock block;
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    block.block_id = LAST_BLOCK_ID;
    signers[sending_id].sign(block);

    // Change the replica ID after signing.
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id + 1;
    ASSERT_FALSE(verifier.verify(block));

    // Make sure it works with the correct replica ID.
    block.replica = REPLICA_PRINCIPAL_ID_START + sending_id;
    ASSERT_TRUE(verifier.verify(block));

    // Change the block ID after signing.
    block.block_id = LAST_BLOCK_ID + 1;
    ASSERT_FALSE(verifier.verify(block));

    // Make sure it works with the correct block ID.
    block.block_id = LAST_BLOCK_ID;
    ASSERT_TRUE(verifier.verify(block));

    // Change a single byte from the signature and make sure it doesn't verify.
    block.signature[0] += 1;
    ASSERT_FALSE(verifier.verify(block));
  }

  // Change the sender in PruneRequest after signing and verify.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }

    request.sender = request.sender + 1;

    ASSERT_TRUE(verifier.verify(request));
  }

  // Verify a PruneRequest with replica_count - 1 latest prunable blocks.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count - 1; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }

    ASSERT_FALSE(verifier.verify(request));
  }

  // Change replica in a single latest prunable block message after signing it
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }
    request.latest_prunable_block[0].replica = REPLICA_PRINCIPAL_ID_START + replica_count + 8;

    ASSERT_FALSE(verifier.verify(request));
  }

  // Invalid (zero) tick_period_seconds and batch_blocks_num.
  {
    concord::messages::PruneRequest request;
    request.sender = CLIENT_PRINCIPAL_ID_START + client_proxy_count * sending_id;
    request.tick_period_seconds = 0;
    request.batch_blocks_num = BATCH_BLOCKS_NUM;
    for (auto i = 0; i < replica_count; ++i) {
      auto &block = request.latest_prunable_block.emplace_back(concord::messages::LatestPrunableBlock());
      block.replica = REPLICA_PRINCIPAL_ID_START + i;
      block.block_id = LAST_BLOCK_ID;
      signers[i].sign(block);
    }
    ASSERT_FALSE(verifier.verify(request));
    request.tick_period_seconds = TICK_PERIOD_SECONDS;
    request.batch_blocks_num = 0;
    ASSERT_FALSE(verifier.verify(request));
  }
}

TEST_F(test_rocksdb, sm_latest_prunable_request_correct_num_bocks_to_keep) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = 1;
  replicaConfig.pruningEnabled_ = true;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = RSAPruningVerifier{replicaConfig.publicKeysOfReplicas};
  replicaConfig.replicaPrivateKey = privateKey_1;
  InitBlockchainStorage(replica_count, storage);

  // Construct the pruning state machine with a nullptr TimeContract to verify
  // it works in case the time service is disabled.
  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlock resp;
  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);
  resp = std::get<concord::messages::LatestPrunableBlock>(rres.response);
  CheckLatestPrunableResp(resp, replica_idx, verifier);
  ASSERT_EQ(resp.block_id, LAST_BLOCK_ID - num_blocks_to_keep);
}
TEST_F(test_rocksdb, sm_latest_prunable_request_big_num_blocks_to_keep) {
  const auto num_blocks_to_keep = LAST_BLOCK_ID + 42;
  const auto replica_idx = 1;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = 1;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = privateKey_1;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = RSAPruningVerifier{replicaConfig.publicKeysOfReplicas};

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlock resp;
  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);
  resp = std::get<concord::messages::LatestPrunableBlock>(rres.response);
  CheckLatestPrunableResp(resp, replica_idx, verifier);
  // Verify that the returned block ID is 0 when pruning_num_blocks_to_keep is
  // bigger than the latest block ID.
  ASSERT_EQ(resp.block_id, 0);
}
// The blockchain created in this test is the same as the one in the
// sm_latest_prunable_request_time_range test.
TEST_F(test_rocksdb, sm_latest_prunable_request_no_pruning_conf) {
  uint64_t replica_count = 4;
  const auto num_blocks_to_keep = 0;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = 1;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = privateKey_1;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  const auto verifier = RSAPruningVerifier{replicaConfig.publicKeysOfReplicas};

  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::LatestPrunableBlock resp;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);
  resp = std::get<concord::messages::LatestPrunableBlock>(rres.response);
  CheckLatestPrunableResp(resp, 1, verifier);
  // Verify that when pruning is enabled and both pruning_num_blocks_to_keep and
  // duration_to_keep_minutes are set to 0, then LAST_BLOCK_ID will be returned.
  ASSERT_EQ(resp.block_id, LAST_BLOCK_ID);
}

TEST_F(test_rocksdb, sm_latest_prunable_request_pruning_disabled) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 0;
  const auto replica_idx = 1;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = false;
  replicaConfig.replicaPrivateKey = privateKey_1;
  auto storage = TestStorage(db);
  auto &blocks_deleter = storage;
  const auto verifier = RSAPruningVerifier{replicaConfig.publicKeysOfReplicas};

  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  concord::messages::LatestPrunableBlockRequest req;
  concord::messages::LatestPrunableBlock resp;
  concord::messages::ReconfigurationResponse rres;
  sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);

  // Verify that when pruning is disabled, there is no answer.
  ASSERT_EQ(std::holds_alternative<concord::messages::LatestPrunableBlock>(rres.response), false);
}
TEST_F(test_rocksdb, sm_handle_prune_request_on_pruning_disabled) {
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  const auto client_idx = 0;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = false;
  replicaConfig.replicaPrivateKey = privateKey_1;

  TestStorage storage(db);
  auto &blocks_deleter = storage;
  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  const auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
  concord::messages::ReconfigurationResponse rres;
  auto res = sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);
  ASSERT_TRUE(res);
}
TEST_F(test_rocksdb, sm_handle_correct_prune_request) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  const auto client_idx = 5;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = privateKey_1;

  TestStorage storage(db);
  auto &blocks_deleter = storage;
  InitBlockchainStorage(replica_count, storage);
  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  const auto latest_prunable_block_id = storage.getLastBlockId() - num_blocks_to_keep;
  const auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas, latest_prunable_block_id);
  blocks_deleter.deleteBlocksUntil(latest_prunable_block_id + 1);
  concord::messages::ReconfigurationResponse rres;
  auto res = sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);

  ASSERT_TRUE(res);
}

TEST_F(test_rocksdb, sm_handle_incorrect_prune_request) {
  const auto replica_count = 4;
  const auto num_blocks_to_keep = 30;
  const auto replica_idx = 1;
  const auto client_idx = 5;
  replicaConfig.numBlocksToKeep_ = num_blocks_to_keep;
  replicaConfig.replicaId = replica_idx;
  replicaConfig.pruningEnabled_ = true;
  replicaConfig.replicaPrivateKey = privateKey_1;
  TestStorage storage(db);
  auto &blocks_deleter = storage;
  InitBlockchainStorage(replica_count, storage);

  auto sm = PruningHandler{storage, storage, blocks_deleter, false};

  // Add a valid N + 1 latest prunable block.
  {
    auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
    const auto &block = req.latest_prunable_block[3];
    auto latest_prunnable_block = concord::messages::LatestPrunableBlock();
    latest_prunnable_block.block_id = block.block_id;
    latest_prunnable_block.replica = block.replica;
    latest_prunnable_block.signature = block.signature;
    req.latest_prunable_block.push_back(std::move(latest_prunnable_block));
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }

  // Send N - 1 latest prunable blocks.
  {
    auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
    req.latest_prunable_block.pop_back();
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }

  // Send a latest prunable block with an invalid signature.
  {
    auto req = ConstructPruneRequest(client_idx, private_keys_of_replicas);
    auto &block = req.latest_prunable_block[req.latest_prunable_block.size() - 1];
    block.signature[0] += 1;
    concord::messages::ReconfigurationResponse rres;
    auto res = sm.handle(req, 0, UINT32_MAX, std::nullopt, rres);

    // Expect that the state machine has ignored the message.
    ASSERT_FALSE(res);
  }
}
}  // namespace

int main(int argc, char **argv) {
  setUpKeysConfiguration_4();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
