// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#define CONCORD_BFT_TESTING

#include "PreProcessor.hpp"
#include "OpenTracing.hpp"
#include "Timers.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "InternalReplicaApi.hpp"
#include "communication/CommFactory.hpp"
#include "Logger.hpp"
#include "RequestProcessingState.hpp"
#include "ReplicaConfig.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "gtest/gtest.h"

using namespace std;
using namespace bft::communication;
using namespace bftEngine;
using namespace preprocessor;

namespace {

const uint16_t numOfReplicas_4 = 4;
const uint16_t numOfReplicas_7 = 7;
const uint16_t numOfInternalClients = 4;
const uint16_t fVal_4 = 1;
const uint16_t fVal_7 = 2;
const uint16_t cVal = 0;
const uint16_t numOfRequiredReplies = fVal_4 + 1;
const uint32_t bufLen = 1024;
const uint64_t reqTimeoutMilli = 10;
const uint64_t preExecReqStatusCheckTimerMillisec = 20;
const uint64_t viewChangeTimerMillisec = 80;
const uint16_t reqWaitTimeoutMilli = 50;
const ReqId reqSeqNum = 123456789;
const uint16_t clientId = 12;
const string cid = "abcd";
const concordUtils::SpanContext span;
const NodeIdType replica_0 = 0;
const NodeIdType replica_1 = 1;
const NodeIdType replica_2 = 2;
const NodeIdType replica_3 = 3;
const NodeIdType replica_4 = 4;
PreProcessorRecorder preProcessorRecorder;
std::shared_ptr<concord::performance::PerformanceManager> sdm = make_shared<concord::performance::PerformanceManager>();

uint64_t reqRetryId = 20;

ReplicaConfig& replicaConfig = ReplicaConfig::instance();
char buf[bufLen];
std::unique_ptr<SigManager> sigManager_[numOfReplicas_4];

class DummyRequestsHandler : public IRequestsHandler {
  void execute(ExecutionRequestsQueue& requests,
               const std::string& batchCid,
               concordUtils::SpanWrapper& parent_span) override {
    for (auto& req : requests) {
      req.outActualReplySize = 256;
      req.outExecutionStatus = 0;
    }
  }
};

class DummyReceiver : public IReceiver {
 public:
  virtual ~DummyReceiver() = default;

  void onNewMessage(const NodeNum sourceNode, const char* const message, const size_t messageLength) override {}
  void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override {}
};

shared_ptr<IncomingMsgsStorage> msgsStorage;
shared_ptr<DummyReceiver> msgReceiver = make_shared<DummyReceiver>();
shared_ptr<MsgHandlersRegistrator> msgHandlersRegPtr = make_shared<MsgHandlersRegistrator>();
shared_ptr<MsgsCommunicator> msgsCommunicator;
shared_ptr<ICommunication> communicatorPtr;
DummyRequestsHandler requestsHandler;

class DummyReplica : public InternalReplicaApi {
 public:
  explicit DummyReplica(bftEngine::impl::ReplicasInfo replicasInfo) : replicasInfo_(move(replicasInfo)) {}

  const bftEngine::impl::ReplicasInfo& getReplicasInfo() const override { return replicasInfo_; }
  bool isValidClient(NodeIdType) const override { return true; }
  bool isIdOfReplica(NodeIdType) const override { return false; }
  const set<ReplicaId>& getIdsOfPeerReplicas() const override { return replicaIds_; }
  ViewNum getCurrentView() const override { return 0; }
  ReplicaId currentPrimary() const override { return replicaConfig.replicaId; }
  bool isCurrentPrimary() const override { return primary_; }
  bool currentViewIsActive() const override { return true; }
  bool isReplyAlreadySentToClient(NodeIdType, ReqId) const override { return false; }
  bool isClientRequestInProcess(NodeIdType, ReqId) const override { return false; }

  IncomingMsgsStorage& getIncomingMsgsStorage() override { return *incomingMsgsStorage_; }
  util::SimpleThreadPool& getInternalThreadPool() override { return pool_; }
  bool isCollectingState() const override { return false; }

  const ReplicaConfig& getReplicaConfig() const override { return replicaConfig; }
  SeqNum getPrimaryLastUsedSeqNum() const override { return 0; }
  uint64_t getRequestsInQueue() const override { return 0; }
  SeqNum getLastExecutedSeqNum() const override { return 0; }

  void setPrimary(bool primary) { primary_ = primary; };

 private:
  bool primary_ = true;
  IncomingMsgsStorage* incomingMsgsStorage_ = nullptr;
  util::SimpleThreadPool pool_;
  bftEngine::impl::ReplicasInfo replicasInfo_;
  set<ReplicaId> replicaIds_;
};

string privateKey_0 =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100C55B8F7979BF24B335017082BF33EE2960E3A0"
    "68DCDB45CA3017214BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B234748"
    "8B0FA3F99B05CFFBB45F80960669594B58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE50B"
    "EF374FCDCE1C8CEBCA1EF12AF492402A6F56DC9338834162F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B484F72"
    "A2BC8C89F86DB52ECC40763B6298879DE686C9A2A78604A503609BA34B779C4F55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF88292D4C"
    "01F99EFE3CC912D09B020111028201001D05EF73BF149474B4F8AEA9D0D2EE5161126A69C6203EF8162184E586D4967833E1F9BF56C89F68AD"
    "35D54D99D8DB4B7BB06C4EFD95E840BBD30C3FD7A5E890CEF6DB99E284576EEED07B6C8CEBB63B4B80DAD2311D1A706A5AC95DE768F017213B"
    "896B9EE38D2E3C2CFCE5BDF51ABD27391761245CDB3DCB686F05EA2FF654FA91F89DA699F14ACFA7F0D8030F74DBFEC28D55C902A27E9C03AB"
    "1CA2770EFC5BE541560D86FA376B1A688D92124496BB3E7A3B78A86EBF1B694683CDB32BC49431990A18B570C104E47AC6B0DE5616851F4309"
    "CFE7D0E20B17C154A3D85F33C7791451FFF73BFC4CDC8C16387D184F42AD2A31FCF545C3F9A498FAAC6E94E902818100F40CF9152ED4854E1B"
    "BF67C5EA185C52EBEA0C11875563AEE95037C2E61C8D988DDF71588A0B45C23979C5FBFD2C45F9416775E0A644CAD46792296FDC68A98148F7"
    "BD3164D9A5E0D6A0C2DF0141D82D610D56CB7C53F3C674771ED9ED77C0B5BF3C936498218176DC9933F1215BC831E0D41285611F512F68327E"
    "4FBD9E5C5902818100CF05519FD69D7C6B61324F0A201574C647792B80E5D4D56A51CF5988927A1D54DF9AE4EA656AE25961923A0EC046F1C5"
    "69BAB53A64EB0E9F5AB2ABF1C9146935BA40F75E0EB68E0BE4BC29A5A0742B59DF5A55AB028F1CCC42243D2AEE4B74344CA33E72879EF2D1CD"
    "D874A7F237202AC7EB57AEDCBD539DEFDA094476EAE613028180396C76D7CEC897D624A581D43714CA6DDD2802D6F2AAAE0B09B885974533E5"
    "14D6167505C620C51EA41CA70E1D73D43AA5FA39DA81799922EB3173296109914B98B2C31AAE515434E734E28ED31E8D37DA99BA11C2E693B6"
    "398570ABBF6778A33C0E40CC6007E23A15C9B1DE6233B6A25304B91053166D7490FCD26D1D8EAC5102818079C6E4B86020674E392CA6F6E5B2"
    "44B0DEBFBF3CC36E232F7B6AE95F6538C5F5B0B57798F05CFD9DFD28D6DB8029BB6511046A9AD1F3AE3F9EC37433DFB1A74CC7E9FAEC08A79E"
    "D9D1D8187F8B8FA107B08F7DAFE3633E1DCC8DC9A0C8689EB55A41E87F9B12347B6A06DB359D89D6AFC0E4CA2A9FF6E5E46EF8BA2845F39665"
    "0281802A89B2BD4A665A0F07DCAFA6D9DB7669B1D1276FC3365173A53F0E0D5F9CB9C3E08E68503C62EA73EB8E0DA42CCF6B136BF4A85B0AC4"
    "24730B4F3CAD8C31D34DD75EF2A39B6BCFE3985CCECC470CF479CF0E9B9D6C7CE1C6C70D853728925326A22352DF73B502D4D3CBC2A770DE27"
    "6E1C5953DF7A9614C970C94D194CAE9188";
string privateKey_1 =
    "308204BC020100300D06092A864886F70D0101010500048204A6308204A20201000282010100CAF26C09B1F6F056F66B54AA0EA002EA236151"
    "3A721A58E44472ADE74DBD833890ED9EE8E0CFF1B84C3DFE9E6226C8CEBFFEDFE71A6BCFE5A599D02FD9940997E864D400BF440BA7B1A568A5"
    "ACF7688B025B5B207E5651E597141FAEF733FA9D87732B8FBA31E082BE777AEB992B00873764655C3B1F8FBF8DD2446F4F9F79883E21DBC1A1"
    "7431E42AF01B9D4DE54B9880E7FB397655C79D7C3A5981CA64A32E2F05FCF0B127F4C6D349056276992B7403D36E03D14C45B6F5F66786280C"
    "921236A17EE0F311821BDF925A47DCA590BB31C60646692014317BE0DD32248DF557A77F80F336C82461B659266F74F495BA5CE194043505B5"
    "F0C363263BF73BB897020111028201005F8123C853BF8028EC6EBE6E2500015F1FB55366CC48A24D4D6324A915865BDE6251B4315ABC3583E7"
    "A4B40E4C4E7C9D8786FFF448AB34A84DEE079E0C096DED221154B50EB69C12ADF37C8A334740416A85580F4A82F95CFBCD3C1619FA57D1A927"
    "238EEE3596D41D656705754169A90B021194D08752B47EF9899DCB1DDED5DCDDA688D9FA88F9303E99CAB040D2E47DCE866717580C2CD01DB9"
    "4E8FE215EAA254432E3B399B90260255DEE7FADED4643814C0DE41DE237BBB7173CFF16AC9237AADA4595FD95EB92E30D2127C7761C6A1CD75"
    "C1F12F5B92B6602E1CDC06E112321B0A41C8A102D785B7C4C7D2441DA983346B81873984DD680FB4B1C9AD7102818100D5E41BE52CECFB7405"
    "01B87C631DEFB3DD855EE95553A816BE8D3DFAE200086E1C12E9FBDDD8737F2CCE2CED1D82431ADC63C1B34EA59C27E98283EB20EAF8F5A29D"
    "45ACE2E5C3173F396DF4356D54F3ED0CF9BBA222E3B9194CD15F88B6AB8FAFDFACACCC97D87555CD1E864D09DF795FB8BE3F9BE2CB8E52F641"
    "FCEDC8E97902818100F2E6BDF9A552D35E9F695C52343D9BBF180BBEB50F6705A7836DF1BFF6A42C2D7A000432957516B555B5E1FBAC21CED5"
    "D2788036AA5AB183A5859284ED409631289F8836D240111B56D6C4953FEFBE177EA137F08ADCABD5CAD07F709E83BB29B0F55AD09E65F5C656"
    "8FE166FF4BE581F4F2066025E3902819EFC2DF0FA63E8F0281803EE8BCE90D36A44F4CC44551C2CC91CB7D637644A0A022610ADE3F67E81E20"
    "98DB149F2BF5F45E347696FE279F446E16F586C080081297570871AE5436DBB2A2993D50BA60DA2A5221A77AB13CE3EBCF45B885AFA8286118"
    "52BC3D94919F23667F058D23C3B4309AFB1E36278011F66EFE0928E58833A547FA486DC2DC8662C902818100AB75B346CF0D49E870869B8552"
    "0D5EE13E26687FCEA3130CD53E8C8780EC5B6B652D3023B4CB1F1696DABDA2979F64D32B27E208784004D565C7B2B82F006A0495255117A378"
    "848BC4D3D60EFFF4862EB3BD186D8F325B2D801AB44F7EF3932C7CE96D47F75707D74C2953D03BBD1A79DA1440BC56FAFC588AC75C6138391D"
    "1902818100BBC05F561AEF4BDE1ECABB5CD313E6173F5E46AC85B4462A8CA689E4E84B95E66733081518DB01714B7B18E44B780DB5A6DA2612"
    "506F46E4D91C8CB582AFD54C7509C9947EC754BFDD0D35FAECE2E729DBD21A62E33C3DEF556913ECF4C8D75910E860D36AFD0977FF9114ACEA"
    "8F44882259C6F1D8314B653F64F4655CFD68A6";
string privateKey_2 =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100DA68B2830103028D0ECD911D20E732257F8A39"
    "BC5214B670C8F1F836CD1D88E6421ABD3C47F16E37E07DCFE89FC11ECBED73896FC978B7E1519F59318F3E24BB5C8962B0EA44889C3A81021C"
    "C9E8EAA94099B9305B0F7731C27CC528CFEE710066CB69229F8AA04DBD842FA8C3143CB0EF1164E45E34B114FBA610DBDFC7453DAD995743AD"
    "1489AA131D0C730163ECA7317048C1AD1008ED31A32CB773D94B5317CAAF95674582DCA1AC9FEBE02079836B52E1E3C8D39571018A7657650D"
    "52D2DF3363F88A690628D64DCBECB88BBA432F27C32E7BC5BC0F1D16D0A027D1D40362F8CEDBE8BED05C2BCE96F33A75CD70014626B32E5EE8"
    "1EBD3116F6F1E5231B02011102820100201E749ACB716241EB96B375398B6941BFEEAE23393F480186F668444B572AB873220CC519A3812655"
    "B8261AAE14DEE1C1097617F7FB2A199B0FE7783AB650B22432524731828C8F7203E9B8F08422824D43C868FE55190ED8D61CFE78EE5BE97887"
    "5339CC2AF974D81AF7F32BBF361A050A165DD19E5646D9B68A02377F2FD417BE92D2722CEE33B3A0F7B111639C092B4024BB08ACEBE9F9BC28"
    "C14BB611DBE627136B568493C6995D81442F09DFEFBA4378A34DEF28F3BD862F4C28BECA87A95551FC2DB48766F82752C7E8B34E8599590190"
    "B12A648AC1E361FE28A3253EC615381A066BFD351C18F0B236429A950106DD4F1134A3DE6CEB1094B7CE5C7102818100EA51580294BB0FD7DA"
    "FDC00364A9B3C4781F70C4BBE8BDE0D82895ADE549E25388E78A53EF83CF9A3D9528F70CB5A57A9A187D5AC68682F03B26C2F6A409E1554FED"
    "061BDC944985102F2988A5A8CD639BA7890FD689D7171612159AAA818F5502EF80D5D13339826A98914A321B5B4512EC24AF3EE80F982D3418"
    "0C85B3E38102818100EE9E7F43A98E593F72A584EEC014E0A46003116B82F5D3A21DAE4EB3F21FBC5B71D96E012B6F5FFBEACED4BEC6C147AA"
    "AB6F9698F0592F3A8A6CD827ABF210497635639043D5F0B461E00914B152D6ECB3EFC9138A1B9FAEE09420AB9C2E1436B6AC36EEB8AD43709B"
    "BFA0ED30FBF09C1A91BAB71410E49E11BE8E2A571C649B02818044EABF8849DCAA4E8BB40B4C4AC8802AB9EB212ACDDB0AAB8ADEC29C8EBB60"
    "AF284419A0376300D3030DC0C121DB128D789DCA841C45AE0A6BC01B397B8A6F7371DC4D1740E051DBD79566919A2296C2F18BA0C86C46A8AC"
    "6FE73387D7CBC0BEA682AD6C105A5C356AA557E8A553571450DC0ACA218F8C1DB2F1343FEB16CA71028180704A963DF57029FFBD7B11614B55"
    "1E6B7879EA1479DD184C4A33E8CD26A585D0AE0BF7881470A5A3B9CABE77E50FA941419DEC8434DEACD04124297C14AE25C837A0A752F2BF07"
    "DC6A4B4F91446337F6EB43A9EB13D0C39D96DC4B9C0D42DC55FB9C5615FC8DC5622B2D006F9E94AD76A31766ECBE26113B53A4F79B744998C1"
    "028180124E2A5DAC7AC7017647FE1B6871484B7B38A3C060B992B871939C8A75BC35DB771F041910C0092E01B545E1910109AA827F34F56327"
    "15E06819ADED60117FD8B9400C3F307EA022D5AC5394E65D28EF434DC068494D33ACF70B43791C1C13C35AC680E0038DB411ADEBBC067F477E"
    "01E8CF793FB076AE47BD83B935B8041DC1";
string privateKey_3 =
    "308204BB020100300D06092A864886F70D0101010500048204A5308204A10201000282010100B4D984129ECC2F4350596DD602EB5337B78E33"
    "C4D945C70C2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F202"
    "E474AF535E92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD185E"
    "1CF3E78B6B82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF4B"
    "C381D21F9ABF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7C9"
    "AA1224D190B2D8A83B020111028201004FC95FEA18E19C6176459256E32B95A7A3CDCB8B8D08322B04A6ACE790BDC5BC806C087D19F2782DDB"
    "0B444C0BC6710ED9564E8073061A66F0D163F5E59D8DB764C3C8ECC523BD758B13815BA1064D597C8C078AF7A450241FED30DDAFEF2CF4FC48"
    "ABD336469D648B4DB70C1A2AF86D9BDC56AC6546C882BD763A963329844BF0E8C79E5E7A5C227A1CDB8473965090084BEAC728E4F86670BAE0"
    "5DA589FAE1EBC98A26BF207261EECE5A92759DE516306B0B2F715370586EF45447F82CC37206F70D762AAB75215482A67FE6742E9D644AB765"
    "4F02476EAE15A742FCB2266DAE437DC76FB17E1698D4987945C779A4B89F5FB3F5E1B194EFA4024BA3797F2502818100E2BDA6F0031EAA027B"
    "1E8099D9D2CF408B8C9D83A22BFBAAD3BCFA952469A001C25E5E2E89DAF28636633E8D7C5E1E5FD8384767DB5B384DAB38147EA46871D5E31E"
    "DC110026E42E90C44F02110A369ACF0445C617CC25CD1207F116B9AF2FA78A89E6C6345EAFD0607CB145410DD6619AC0C034B54283A0065B93"
    "3550B8DACF02818100CC2FDB49EB3E45DB2EB644048C3C35E3BB50980453DB8EB54DD5595CA2DBC43A2F2912D0F696E6126AFBF5D7777BABB2"
    "6AC931030B8896343BC19EA30B8EEBFECE2617A2564B3D66E29DF66708254877CC33E699501A2BA4D0D7D02F068B1DCF6C7A0794F24BEE83BE"
    "2E8453D3E436C3B59D2D9BEEB5B38541EF170544EA46D502818100AD63DA02D5359110F4BCF8EE1F0A9E7CA6F30F0A4ED6570A29726544DF9C"
    "10F2495738F6696B31EE29972FD59B57082B2CDFBE223E54D0B3DD49009D144FDE94808102A396B454239BE169982B25ED85712162886C8D0D"
    "D90DC9D67ACA3AABF8971E28F1EBCFEFDB95140F16D764EF3B947547AFD5E791D4B9915274108D5C07028180300B42A7FB1DB61574671F1020"
    "FF1BBD1D03E7888C33A91B99D7D8CA80AC2E2BCEDC7CE5DFAB08F546596705858682C09198C03CF3A7AADF1D1E7FADE49A196921725FE9F62F"
    "D236537076365C4501FE11EE182412D8FB35D6C95E292EB7524EEC58F2B9A26C381EFF92797D22CC491EFD8E6515A1942A3D78ECF65B97BEA7"
    "410281806625E51F70BB9E00D0260941C56B22CDCECC7EA6DBC2C28A1CDA0E1AFAACD39C5E962E40FCFC0F5BBA57A7291A1BDC5D3D29495B45"
    "CE2B3BEC7748E6C351FF934537BCA246CA36B840CF68BE6D473F7EE079B0EFDFF6B1BB554B06093E18B269FC3BD3F9876376CA7C673A93F142"
    "7088BF0990AB8E232F269B5DBCD446385A66";
string privateKey_4 =
    "308204BB020100300D06092A864886F70D0101010500048204A5308204A10201000282010100B4D984129ECC2F4350596DD602EB5337B78E33"
    "C4D945C70C2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F202"
    "E474AF535E92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD185E"
    "1CF3E78B6B82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF4B"
    "C381D21F9ABF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7C9"
    "AA1224D190B2D8A83B020111028201004FC95FEA18E19C6176459256E32B95A7A3CDCB8B8D08322B04A6ACE790BDC5BC806C087D19F2782DDB"
    "0B444C0BC6710ED9564E8073061A66F0D163F5E59D8DB764C3C8ECC523BD758B13815BA1064D597C8C078AF7A450241FED30DDAFEF2CF4FC48"
    "ABD336469D648B4DB70C1A2AF86D9BDC56AC6546C882BD763A963329844BF0E8C79E5E7A5C227A1CDB8473965090084BEAC728E4F86670BAE0"
    "5DA589FAE1EBC98A26BF207261EECE5A92759DE516306B0B2F715370586EF45447F82CC37206F70D762AAB75215482A67FE6742E9D644AB765"
    "4F02476EAE15A742FCB2266DAE437DC76FB17E1698D4987945C779A4B89F5FB3F5E1B194EFA4024BA3797F2502818100E2BDA6F0031EAA027B"
    "1E8099D9D2CF408B8C9D83A22BFBAAD3BCFA952469A001C25E5E2E89DAF28636633E8D7C5E1E5FD8384767DB5B384DAB38147EA46871D5E31E"
    "DC110026E42E90C44F02110A369ACF0445C617CC25CD1207F116B9AF2FA78A89E6C6345EAFD0607CB145410DD6619AC0C034B54283A0065B93"
    "3550B8DACF02818100CC2FDB49EB3E45DB2EB644048C3C35E3BB50980453DB8EB54DD5595CA2DBC43A2F2912D0F696E6126AFBF5D7777BABB2"
    "6AC931030B8896343BC19EA30B8EEBFECE2617A2564B3D66E29DF66708254877CC33E699501A2BA4D0D7D02F068B1DCF6C7A0794F24BEE83BE"
    "2E8453D3E436C3B59D2D9BEEB5B38541EF170544EA46D502818100AD63DA02D5359110F4BCF8EE1F0A9E7CA6F30F0A4ED6570A29726544DF9C"
    "10F2495738F6696B31EE29972FD59B57082B2CDFBE223E54D0B3DD49009D144FDE94808102A396B454239BE169982B25ED85712162886C8D0D"
    "D90DC9D67ACA3AABF8971E28F1EBCFEFDB95140F16D764EF3B947547AFD5E791D4B9915274108D5C07028180300B42A7FB1DB61574671F1020"
    "FF1BBD1D03E7888C33A91B99D7D8CA80AC2E2BCEDC7CE5DFAB08F546596705858682C09198C03CF3A7AADF1D1E7FADE49A196921725FE9F62F"
    "D236537076365C4501FE11EE182412D8FB35D6C95E292EB7524EEC58F2B9A26C381EFF92797D22CC491EFD8E6515A1942A3D78ECF65B97BEA7"
    "410281806625E51F70BB9E00D0260941C56B22CDCECC7EA6DBC2C28A1CDA0E1AFAACD39C5E962E40FCFC0F5BBA57A7291A1BDC5D3D29495B45"
    "CE2B3BEC7748E6C351FF934537BCA246CA36B840CF68BE6D473F7EE079B0EFDFF6B1BB554B06093E18B269FC3BD3F9876376CA7C673A93F142"
    "7088BF0990AB8E232F269B5DBCD446385A66";
string publicKey_0 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100C55B8F7979BF24B335017082BF33EE2960E3A068DCDB45CA"
    "3017214BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B2347488B0FA3F99B"
    "05CFFBB45F80960669594B58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE50BEF374FCDCE"
    "1C8CEBCA1EF12AF492402A6F56DC9338834162F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B484F72A2BC8C89F8"
    "6DB52ECC40763B6298879DE686C9A2A78604A503609BA34B779C4F55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF88292D4C01F99EFE3C"
    "C912D09B020111";
string publicKey_1 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100CAF26C09B1F6F056F66B54AA0EA002EA2361513A721A58E4"
    "4472ADE74DBD833890ED9EE8E0CFF1B84C3DFE9E6226C8CEBFFEDFE71A6BCFE5A599D02FD9940997E864D400BF440BA7B1A568A5ACF7688B02"
    "5B5B207E5651E597141FAEF733FA9D87732B8FBA31E082BE777AEB992B00873764655C3B1F8FBF8DD2446F4F9F79883E21DBC1A17431E42AF0"
    "1B9D4DE54B9880E7FB397655C79D7C3A5981CA64A32E2F05FCF0B127F4C6D349056276992B7403D36E03D14C45B6F5F66786280C921236A17E"
    "E0F311821BDF925A47DCA590BB31C60646692014317BE0DD32248DF557A77F80F336C82461B659266F74F495BA5CE194043505B5F0C363263B"
    "F73BB897020111";
string publicKey_2 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100DA68B2830103028D0ECD911D20E732257F8A39BC5214B670"
    "C8F1F836CD1D88E6421ABD3C47F16E37E07DCFE89FC11ECBED73896FC978B7E1519F59318F3E24BB5C8962B0EA44889C3A81021CC9E8EAA940"
    "99B9305B0F7731C27CC528CFEE710066CB69229F8AA04DBD842FA8C3143CB0EF1164E45E34B114FBA610DBDFC7453DAD995743AD1489AA131D"
    "0C730163ECA7317048C1AD1008ED31A32CB773D94B5317CAAF95674582DCA1AC9FEBE02079836B52E1E3C8D39571018A7657650D52D2DF3363"
    "F88A690628D64DCBECB88BBA432F27C32E7BC5BC0F1D16D0A027D1D40362F8CEDBE8BED05C2BCE96F33A75CD70014626B32E5EE81EBD3116F6"
    "F1E5231B020111";
string publicKey_3 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100B4D984129ECC2F4350596DD602EB5337B78E33C4D945C70C"
    "2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F202E474AF535E"
    "92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD185E1CF3E78B6B"
    "82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF4BC381D21F9A"
    "BF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7C9AA1224D190"
    "B2D8A83B020111";
string publicKey_4 =
    "30820120300D06092A864886F70D01010105000382010D00308201080282010100B4D984129ECC2F4350596DD602EB5337B78E33C4D945C70C"
    "2CACFF6237037BEF897D6893079D1067F07FF023D66C77BB41F6D41215C9912D995215C6F7651F1728AAB65CF20CBE81E5E7F202E474AF535E"
    "92BBC386A7A496263BC4189FD9DD5E801C3023038EFE5B5DE35AA3F70C10F87259D586C36D3DF524E9DA2140C481365985AD185E1CF3E78B6B"
    "82FDD274CCD1267838A0B91F9B48544AFB2C71B158A26E6154CECB43ECEE13FFBD6CA91D8D115B6B91C55F3A88E3F389E9A2AF4BC381D21F9A"
    "BF2EA16F58773514249B03A4775C6A10561AFC8CF54B551A43FD014F3C5FE12D96AC5F117645E26D125DC7430114FA60577BF7C9AA1224D190"
    "B2D8A83B020111";

void setUpConfiguration_4() {
  replicaConfig.replicaId = replica_0;
  replicaConfig.numReplicas = numOfReplicas_4;
  replicaConfig.fVal = fVal_4;
  replicaConfig.cVal = cVal;
  replicaConfig.numOfClientProxies = numOfInternalClients;
  replicaConfig.viewChangeTimerMillisec = viewChangeTimerMillisec;
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.numOfExternalClients = 15;
  replicaConfig.clientBatchingEnabled = true;

  replicaConfig.publicKeysOfReplicas.insert(pair<uint16_t, const string>(replica_0, publicKey_0));
  replicaConfig.publicKeysOfReplicas.insert(pair<uint16_t, const string>(replica_1, publicKey_1));
  replicaConfig.publicKeysOfReplicas.insert(pair<uint16_t, const string>(replica_2, publicKey_2));
  replicaConfig.publicKeysOfReplicas.insert(pair<uint16_t, const string>(replica_3, publicKey_3));
  replicaConfig.replicaPrivateKey = privateKey_0;
  string privateKeys[numOfReplicas_4] = {privateKey_0, privateKey_1, privateKey_2, privateKey_3};

  for (auto i = 0; i < replicaConfig.numReplicas; i++) {
    sigManager_[i].reset(SigManager::initInTesting(i,
                                                   privateKeys[i],
                                                   replicaConfig.publicKeysOfReplicas,
                                                   KeyFormat::HexaDecimalStrippedFormat,
                                                   nullptr,
                                                   KeyFormat::HexaDecimalStrippedFormat,
                                                   replicaConfig.numReplicas,
                                                   replicaConfig.numRoReplicas,
                                                   replicaConfig.numOfClientProxies,
                                                   replicaConfig.numOfExternalClients));
  }
  SigManager::setInstance(sigManager_[replicaConfig.replicaId].get());
}

void setUpConfiguration_7() {
  replicaConfig.replicaId = replica_4;
  replicaConfig.numReplicas = numOfReplicas_7;
  replicaConfig.fVal = fVal_7;

  replicaConfig.publicKeysOfReplicas.insert(pair<uint16_t, const string>(replica_4, publicKey_4));
  replicaConfig.replicaPrivateKey = privateKey_4;
}

void setUpCommunication() {
  unordered_map<NodeNum, NodeInfo> nodes;

  NodeInfo nodeInfo{"128.0.0.1", 4321, true};
  nodes[1] = nodeInfo;

  PlainUdpConfig configuration("128.0.0.1", 1234, 4096, nodes, replicaConfig.replicaId);
  communicatorPtr.reset(CommFactory::create(configuration), [](ICommunication* c) {
    c->Stop();
    delete c;
  });
  msgsCommunicator.reset(new MsgsCommunicator(communicatorPtr.get(), msgsStorage, msgReceiver));
  msgsCommunicator->startCommunication(replicaConfig.replicaId);
}

PreProcessReplyMsgSharedPtr preProcessNonPrimary(NodeIdType replicaId, const bftEngine::impl::ReplicasInfo& repInfo) {
  SigManager::setInstance(sigManager_[replicaId].get());
  auto preProcessReplyMsg =
      make_shared<PreProcessReplyMsg>(&preProcessorRecorder, replicaId, clientId, 0, reqSeqNum, reqRetryId);
  preProcessReplyMsg->setupMsgBody(buf, bufLen, "", STATUS_GOOD);
  // preProcessReplyMsg->validate(repInfo);
  SigManager::setInstance(sigManager_[repInfo.myId()].get());
  return preProcessReplyMsg;
}

void clearDiagnosticsHandlers() {
  auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.clear();
  registrar.status.clear();
}

TEST(requestPreprocessingState_test, notEnoughRepliesReceived) {
  RequestProcessingState reqState(replicaConfig.numReplicas,
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  for (auto i = 1; i < numOfRequiredReplies; i++) {
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo));
    ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
  }
  reqState.handlePrimaryPreProcessed(buf, bufLen);
  ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
  clearDiagnosticsHandlers();
}

TEST(requestPreprocessingState_test, allRepliesReceivedButNotEnoughSameHashesCollected) {
  RequestProcessingState reqState(replicaConfig.numReplicas,
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  reqState.handlePrimaryPreProcessed(buf, bufLen);
  for (auto i = 1; i < replicaConfig.numReplicas; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
    memset(buf, i, bufLen);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo));
  }
  ConcordAssert(reqState.definePreProcessingConsensusResult() == CANCEL);
}

TEST(requestPreprocessingState_test, enoughSameRepliesReceived) {
  RequestProcessingState reqState(replicaConfig.numReplicas,
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != numOfRequiredReplies - 1) ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo));
  }
  reqState.handlePrimaryPreProcessed(buf, bufLen);
  ConcordAssert(reqState.definePreProcessingConsensusResult() == COMPLETE);
  clearDiagnosticsHandlers();
}

TEST(requestPreprocessingState_test, primaryReplicaPreProcessingRetrySucceeds) {
  RequestProcessingState reqState(replicaConfig.numReplicas,
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  reqState.handlePrimaryPreProcessed(buf, bufLen);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
    memset(buf, '4', bufLen);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo));
  }
  ConcordAssert(reqState.definePreProcessingConsensusResult() == RETRY_PRIMARY);
  memset(buf, '4', bufLen);
  reqState.handlePrimaryPreProcessed(buf, bufLen);
  ConcordAssert(reqState.definePreProcessingConsensusResult() == COMPLETE);
  clearDiagnosticsHandlers();
}

TEST(requestPreprocessingState_test, primaryReplicaDidNotCompletePreProcessingWhileNonPrimariesDid) {
  RequestProcessingState reqState(replicaConfig.numReplicas,
                                  clientId,
                                  0,
                                  cid,
                                  reqSeqNum,
                                  ClientPreProcessReqMsgUniquePtr(),
                                  PreProcessRequestMsgSharedPtr());
  bftEngine::impl::ReplicasInfo repInfo(replicaConfig, true, true);
  memset(buf, '5', bufLen);
  for (auto i = 1; i <= numOfRequiredReplies; i++) {
    if (i != replicaConfig.numReplicas - 1) ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
    memset(buf, '4', bufLen);
    reqState.handlePreProcessReplyMsg(preProcessNonPrimary(i, repInfo));
  }
  ConcordAssert(reqState.definePreProcessingConsensusResult() == CONTINUE);
  memset(buf, '4', bufLen);
  reqState.handlePrimaryPreProcessed(buf, bufLen);
  ConcordAssert(reqState.definePreProcessingConsensusResult() == COMPLETE);
}

TEST(requestPreprocessingState_test, requestTimedOut) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig, false, false);
  DummyReplica replica(replicasInfo);
  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientPreProcessRequest);
  auto* clientReqMsg = new ClientPreProcessRequestMsg(clientId, reqSeqNum, bufLen, buf, reqTimeoutMilli, cid);
  msgHandlerCallback(clientReqMsg);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == reqSeqNum);
  usleep(replicaConfig.preExecReqStatusCheckTimerMillisec * 1000);
  timers.evaluate();
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 0);
  clearDiagnosticsHandlers();
}

TEST(requestPreprocessingState_test, primaryCrashDetected) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig, false, false);
  DummyReplica replica(replicasInfo);
  replica.setPrimary(false);
  concordUtil::Timers timers;
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientPreProcessRequest);
  auto* clientReqMsg = new ClientPreProcessRequestMsg(clientId, reqSeqNum, bufLen, buf, reqTimeoutMilli, cid);
  msgHandlerCallback(clientReqMsg);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == reqSeqNum);

  usleep(reqWaitTimeoutMilli * 1000);
  timers.evaluate();
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 0);
  clearDiagnosticsHandlers();
}

TEST(requestPreprocessingState_test, primaryCrashNotDetected) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig, false, false);
  DummyReplica replica(replicasInfo);
  replica.setPrimary(false);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientPreProcessRequest);
  auto* clientReqMsg = new ClientPreProcessRequestMsg(clientId, reqSeqNum, bufLen, buf, reqTimeoutMilli, cid);
  msgHandlerCallback(clientReqMsg);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == reqSeqNum);

  auto* preProcessReqMsg =
      new PreProcessRequestMsg(replica.currentPrimary(), clientId, 0, reqSeqNum, reqRetryId, bufLen, buf, cid, span);
  msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::PreProcessRequest);
  msgHandlerCallback(preProcessReqMsg);
  usleep(reqWaitTimeoutMilli * 1000 / 2);  // Wait for the pre-execution completion
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 0);
  clearDiagnosticsHandlers();
}

TEST(requestPreprocessingState_test, batchMsgTimedOutOnNonPrimary) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig, false, false);
  DummyReplica replica(replicasInfo);
  replica.setPrimary(false);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientBatchRequest);
  deque<ClientRequestMsg*> batch;
  uint batchSize = 0;
  for (uint i = 0; i < 3; i++) {
    auto* clientReqMsg = new ClientRequestMsg(clientId, 2, i + 5, bufLen, buf, reqTimeoutMilli, to_string(i + 5));
    batch.push_back(clientReqMsg);
    batchSize += clientReqMsg->size();
  }
  auto* clientBatchReqMsg = new ClientBatchRequestMsg(clientId, batch, batchSize, cid);
  msgHandlerCallback(clientBatchReqMsg);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 5);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 1) == 6);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 2) == 7);

  usleep(replicaConfig.preExecReqStatusCheckTimerMillisec * 1000);
  timers.evaluate();
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 0);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 1) == 0);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 2) == 0);
  clearDiagnosticsHandlers();

  for (auto& b : batch) {
    delete b;
  }
}

TEST(requestPreprocessingState_test, batchMsgTimedOutOnPrimary) {
  setUpConfiguration_7();

  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig, false, false);
  DummyReplica replica(replicasInfo);
  replica.setPrimary(true);
  replicaConfig.preExecReqStatusCheckTimerMillisec = preExecReqStatusCheckTimerMillisec;
  replicaConfig.replicaId = replica_1;

  concordUtil::Timers timers;
  PreProcessor preProcessor(msgsCommunicator, msgsStorage, msgHandlersRegPtr, requestsHandler, replica, timers, sdm);

  auto msgHandlerCallback = msgHandlersRegPtr->getCallback(bftEngine::impl::MsgCode::ClientBatchRequest);
  deque<ClientRequestMsg*> batch;
  uint batchSize = 0;
  for (uint i = 0; i < 3; i++) {
    auto* clientReqMsg = new ClientRequestMsg(clientId, 2, i + 5, bufLen, buf, reqTimeoutMilli, to_string(i + 5));
    batch.push_back(clientReqMsg);
    batchSize += clientReqMsg->size();
  }
  auto* clientBatchReqMsg = new ClientBatchRequestMsg(clientId, batch, batchSize, cid);
  msgHandlerCallback(clientBatchReqMsg);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 5);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 1) == 6);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 2) == 7);

  usleep(reqTimeoutMilli * 1000 * 4);
  timers.evaluate();
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 0) == 0);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 1) == 0);
  ConcordAssert(preProcessor.getOngoingReqIdForClient(clientId, 2) == 0);
  clearDiagnosticsHandlers();

  for (auto& b : batch) {
    delete b;
  }
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  logging::initLogger("logging.properties");
  setUpConfiguration_4();
  RequestProcessingState::init(numOfRequiredReplies, &preProcessorRecorder);
  const chrono::milliseconds msgTimeOut(20000);
  msgsStorage = make_shared<IncomingMsgsStorageImp>(msgHandlersRegPtr, msgTimeOut, replicaConfig.replicaId);
  setUpCommunication();
  int res = RUN_ALL_TESTS();
  return res;
}
