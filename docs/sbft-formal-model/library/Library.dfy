module Library {
  function DropLast<T>(theSeq: seq<T>) : seq<T>
    requires 0 < |theSeq|
  {
    theSeq[..|theSeq|-1]
  }

  function Last<T>(theSeq: seq<T>) : T
    requires 0 < |theSeq|
  {
    theSeq[|theSeq|-1]
  }

  function UnionSeqOfSets<T>(theSets: seq<set<T>>) : set<T>
  {
    if |theSets| == 0 then {} else
      UnionSeqOfSets(DropLast(theSets)) + Last(theSets)
  }

  // As you can see, Dafny's recursion heuristics easily complete the recursion
  // induction proofs, so these two statements could easily be ensures of
  // UnionSeqOfSets. However, the quantifiers combine with native map axioms
  // to be a bit trigger-happy, so we've pulled them into independent lemmas
  // you can invoke only when needed.
  // Suggestion: hide calls to this lemma in a an
  //   assert P by { SetsAreSubsetsOfUnion(...) }
  // construct so you can get your conclusion without "polluting" the rest of the
  // lemma proof context with this enthusiastic forall.
  lemma SetsAreSubsetsOfUnion<T>(theSets: seq<set<T>>)
    ensures forall idx | 0<=idx<|theSets| :: theSets[idx] <= UnionSeqOfSets(theSets)
  {
  }

  lemma EachUnionMemberBelongsToASet<T>(theSets: seq<set<T>>)
    ensures forall member | member in UnionSeqOfSets(theSets) ::
          exists idx :: 0<=idx<|theSets| && member in theSets[idx]
  {
  }

  // Convenience function for learning a particular index (invoking Hilbert's
  // Choose on the exists in EachUnionMemberBelongsToASet).
  lemma GetIndexForMember<T>(theSets: seq<set<T>>, member: T) returns (idx:int)
    requires member in UnionSeqOfSets(theSets)
    ensures 0<=idx<|theSets|
    ensures member in theSets[idx]
  {
    EachUnionMemberBelongsToASet(theSets);
    var chosenIdx :| 0<=chosenIdx<|theSets| && member in theSets[chosenIdx];
    idx := chosenIdx;
  }

  datatype Option<T> = Some(value:T) | None

  function {:opaque} MapRemoveOne<K,V>(m:map<K,V>, key:K) : (m':map<K,V>)
    ensures forall k :: k in m && k != key ==> k in m'
    ensures forall k :: k in m' ==> k in m && k != key
    ensures forall j :: j in m' ==> m'[j] == m[j]
    ensures |m'.Keys| <= |m.Keys|
    ensures |m'| <= |m|
  {
    var m':= map j | j in m && j != key :: m[j];
    assert m'.Keys == m.Keys - {key};
    m'
  }


  lemma SubsetCardinality<T>(a:set<T>, b:set<T>)
    requires a <= b
    ensures |a| <= |b|
  {
    var diff := b - a;
    assert b == a + diff;
    assert |b| == |a| + |diff|;
  }

  predicate FullImap<K(!new),V>(im:imap<K,V>) {
    forall k :: k in im
  }

  lemma SingletonSetAxiom<T>(x:T, s:set<T>) 
     requires x in s
     requires |s| == 1
     ensures s == {x}

  // Warning: Dafny automation black magic.
  // Everything is in a FullImap! But sometimes Dafny seems unable
  // to trigger that forall. (jonh suspects the issue is related to
  // the map being a deeply-nested expression, since Dafny gets the
  // proof no problem here where the map is a top-level object.)
  // So this predicate does "nothing" logically (it's just 'true'),
  // but has the 'ensures' side-effect of pointing out the 'in' expression
  // we need.
  predicate TriggerKeyInFullImap<K(!new),V>(k:K, m:imap<K,V>) 
    requires FullImap(m)
    ensures k in m
  {
    true
  }

}
