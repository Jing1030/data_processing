import math # has log function
import re
import pandas as pd 

from scipy import stats # has geometric mean function

def med_norm(df):
    """
        Median normalizes the read counts in the passed in pandas dataframe
    """
    # Calculates the geometric mean of the sgRNA read number across samples
    sgRNADF = df.apply(stats.mstats.gmean, axis=1)
    # Divides the read number by the geometric mean
    divDF = df.div(sgRNADF, axis=0)
    # Gets the median of this ratio for each sample
    medSer = divDF.median(axis=0)
    # Corrects each read number by the median ratio of the sample
    outDF = df.div(medSer, axis=1)
    # Adds one to each number
    outDF = outDF.applymap(lambda x: x+1)
    return outDF

def rpm_norm_df(df):
    """
        Reads per million normalizes the passed in pandas dataframe
    """
    # Calculates the number of reads in each sample
    readTotalDF = df.sum(axis=0)
    # Divides the read number by the total number of reads in the sample
    divDF = df.div(readTotalDF, axis=1)
    # Multiplies each cell by 1000000 and adds 1
    outDF = divDF.applymap(lambda x: x*1000000+1)
    return outDF

def rpm_norm_ser(ser):
    """
        Reads per million normalizes the passed in pandas series
    """
    # Calculates the total number of reads 
    readTotal = ser.sum()
    # Divides the read number by the total number of reads in the sample
    divSer = ser.div(readTotal)
    # Multiplies each cell by 1000000 and adds 1
    outSer = divSer.apply(lambda x: x*1000000+1)
    return outSer

def rpkm_norm(df, gene_len_file):
    """
        Normalizes the mRNA read count by the number of reads in sample
        and by the length of the gene (sum of the exons minus overlap)
    """
    # per million scaling
    samp_sum = df.sum()
    df_rpm = df.div(samp_sum)
    df_rpm = df_rpm.applymap(lambda x: x*10**6)

    # get gene lengths from file into series
    gene_len = pd.read_csv(gene_len_file, header=None, index_col=0, squeeze=True)

    # gene length scaling
    df_rpbm = df_rpm.div(gene_len, axis="index")
    df_rpkm = df_rpbm.applymap(lambda x: x*1000)
    return df_rpkm

def get_gene_len_gtf(gtf_file, out_file):
    """
        Adds together the exon lengths minus any overlap
        Takes a gtf file as input
    """
    with open(gtf_file, "r") as fin:
        gene_name_re = re.compile('; gene_name "(.*?)";')
        gene_exons = {}
        for line in fin:
            if line[0] == "#":
                continue
            ele = line.split("\t")
            if ele[2] != "exon":
                continue
            gene_name = gene_name_re.search(ele[8]).group(1)
            start = int(ele[3])
            end = int(ele[4])
            if gene_name in gene_exons:
                gene_exons[gene_name] += [(start, end)]
            else:
                gene_exons[gene_name] = [(start, end)]

    gene_len = pd.Series(name="GeneLength")
    for gene in gene_exons:
        exons = sorted(gene_exons[gene])
        old_end = 0
        length = 0
        for start, end in exons:
            if old_end > start and end > old_end:
                length += end - old_end
                old_end = end
            elif old_end > start and old_end >= end:
                continue
            else:
                length += end - start
                old_end = end
        gene_len[gene] = length
    gene_len.to_csv(out_file)

def tmm_norm(df, ref_samp, trim_fc_perc=30, trim_abs_perc=5):
    """
        Trimmed mean of M-values normalizes the dataframe to the reference sample
        Python implimentation of EdgeR .calcFactorWeighted
    """
    samps = df.columns.tolist()
    fact_ser = pd.Series(index=samps) # series with factors

    # remove all rows with 0 reads for any sample, this is fine for library estimations 
    # as they would all have extreme fold change 
    filt_df = df
    for samp in samps:
        filt_df = filt_df[filt_df[samp] != 0]
        
    for samp in samps:
        # number reads in sample
        nS = filt_df.loc[:,samp].sum()
        # number reads in ref
        nR = filt_df.loc[:,ref_samp].sum()
        
        # fraction of total reads
        ygk_nS = filt_df.loc[:,samp].div(nS)
        ygk_nR = filt_df.loc[:,ref_samp].div(nR)
        
        # calculate gene-wise log fold change
        two_mg = ygk_nS/ygk_nR 
        mg = two_mg.map(lambda x: math.log(x, 2)) # logR in EdgeR
        
        # rank fold changes
        n_mg = len(mg)
        ranks = mg.rank() # on ties, average of rank, used by R
        mg_keep_genes = ranks[(ranks > ((trim_fc_perc/100.0)*n_mg)) & (ranks < (1.0-(trim_fc_perc/100.0))*n_mg+1)].index.tolist()

        # absolute expression
        two_ag = ygk_nS*ygk_nR
        ag = two_ag.map(lambda x: 0.5*math.log(x, 2)) # absE in EdgeR

        # rank fold changes
        n_ag = len(ag)
        ranks = ag.rank() # on ties, average of rank, used by R
        ag_keep_genes = ranks[(ranks > ((trim_abs_perc/100.0)*n_ag)) & (ranks < (1.0-(trim_abs_perc/100.0))*n_ag+1)].index.tolist()

        # genes to keep after both trimming
        keep_genes = [gene for gene in mg_keep_genes if gene in ag_keep_genes]

        # approximate asymptotic variance
        wrk_left = filt_df.loc[:, samp].map(lambda x: (nS-x)/float((nS*x)))
        wrk_right = filt_df.loc[:, ref_samp].map(lambda x: (nR-x)/float((nR*x)))
        wrk = wrk_left + wrk_right # v in EdgeR

        de_calc_first = mg.loc[keep_genes].div(wrk.loc[keep_genes]).sum()
        de_calc_sec = wrk.loc[keep_genes].map(lambda x: 1/x).sum()
        de_calc = 2**(de_calc_first/de_calc_sec)
        fact_ser[samp] = de_calc
    # to make factors multiply to one, divide by geometric mean
    geo_mean = stats.mstats.gmean(fact_ser.values.tolist())
    fact_ser_scaled = fact_ser.map(lambda x: x/geo_mean)
    scale_counts = df.sum()*fact_ser_scaled
    # Divides the read number by the scaled number of reads in the sample
    div_df = df.div(scale_counts)
    # Multiplies each cell by 1000000
    out_df = div_df.apply(lambda x: x*1000000.0)
    return out_df