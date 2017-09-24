from string import maketrans

def rev_comp(seq):
    """
        Reverse complements the passed in DNA or RNA sequence
    """
    t = maketrans("ATCGUatcgu", "TAGCAtagca")
    new_seq = seq.translate(t)[::-1]
    return new_seq

def transcribe(seq):
    """
        Transcribes a DNA sequence into RNA
    """
    t = maketrans("ATCGatcg", "UAGCuagc")
    new_seq = seq.translate(t)[::-1]
    return new_seq

def rev_trans(seq):
    """
        Reverse transcribes a RNA sequence into DNA
    """
    t = maketrans("AUCGaucg", "TAGCtagc")
    new_seq = seq.translate(t)[::-1]
    return new_seq

def dna_to_rna(seq):
    """
        Directly converts DNA to RNA by replacing T with U
    """
    new_seq = seq.replace("t", "u")
    new_seq = new_seq.replace("T", "U")
    return new_seq

def rna_to_dna(seq):
    """
        Directly converts RNA to DNA by replacing U with T
    """
    new_seq = seq.replace("u", "t")
    new_seq = new_seq.replace("U", "T")
    return new_seq