import random
import pkg_resources
import pandas as pd

from collections import Counter

def create_mir_dict():
    """
        Creates a dictionary of chromosomes which hold a positive strand and negative strand dictionary
        which each hold a dictionary with genomic locations as keys and mature miRNA IDs of the miRNAs 
        which align to that location as values.
        
        Also creates a dictionary with every matID as keys and 0 as values to intialize counter object
    """
    mirna_file = pkg_resources.resource_filename("data_processing", "data/MatureMiR_aligned.sam")

    # Create a dictionary with chromosome keys and +/- strand dictionary values
    chrom_dict ={}
    # Create ditionary to initialize counter
    mat_ids = {}
    # Load a list with the primary miRNA genome start, genome end and name into the appropriate chr
    # and strand dictionary
    with open(mirna_file, "r") as fin:
        for line in fin:
            # skip header line
            if line[0] == "@":
                continue
            ele = line.split("\t")
            chromosome = ele[2]
            mirID = ele[0]
            start = int(ele[3])
            if chromosome not in chrom_dict:
                chrom_dict[ele[2]] = {"+": {}, "-": {}}
            if ele[1] == "16" or ele[1] == "272":
                strand = "-"
                start_mir = start+len(ele[9])-1 # first base of the mature miRNA
            else:
                strand = "+"
                start_mir = start
            # the start of the alignment minus 5 to the end plus 5
            loc_list = range(start-5,start+len(ele[9])+5)
            for loc in loc_list:
                if loc in chrom_dict[chromosome][strand]:
                    chrom_dict[chromosome][strand][loc] += [(mirID, start_mir)]
                else:
                    chrom_dict[chromosome][strand][loc] = [(mirID, start_mir)]
            # start with count 0
            if mirID not in mat_ids:
                mat_ids[mirID] = 0
    return chrom_dict, mat_ids
    
def find_mir_match(f_name, chrom_dict, mat_ids):
    """
        Takes a file name of sam file, chromosome dictionary,
        and dictionary with mature miRNA IDs keys and values=0 
    """
    chroms = chrom_dict.keys() # fetch the list of chromosomes
    
    # Check to make sure the passed in file is a sam file
    if ".sam" not in f_name:
        return False, False
        
    # Create a counter object with mature miRNA miRBase IDs as keys and with initial value set to 0
    read_counter = Counter(mat_ids)
    qc_counter = Counter() # keeps track of the number of reads covering each part of the miRNA
    with open(f_name, "rb") as f_in:
        for line in f_in:
            # skip comment lines
            if line[0] == "@":
                continue
            ele = line.split("\t")
            read_name = ele[0]
            chrom = ele[2]
            al_start = int(ele[3])
            al_seq = ele[9]
            al_end = al_start + len(al_seq) - 1
            if ele[1] =="16": # sam flag for reverse strand 
                al_strand = "-"
            elif ele[1] == "0": # no flags, forward strand
                al_strand = "+"
            elif ele[1] == "4": # sam flag for no alignment
                continue
            else:
                print "Unknown sam flag '{}'".format(ele[1])
                continue

            if chrom in chroms:
                pos_mat = chrom_dict[chrom][al_strand]
                mats = []
                for loc in range(al_start, al_end):
                    # Check if the location is a key with a corresponing miRNA value
                    if loc in pos_mat:
                        mats += pos_mat[loc]
                # Stop if the alignment location does not overlap with any miRNAs
                if mats == []:
                    continue

                n = len(set(mats)) # number of mature miRNAs read overlaps
                if n == 1:
                    (mat,) = set(mats) # the comma tells python its a tuple
                else:
                    # Want the mature miRNA with the greatest overlap
                    mat_counts = Counter(mats).most_common()
                    greatest = mat_counts[0][1] # the largest overlap
                    pos_mats = [mat_counts[0][0]]

                    # loop over mature miRNAs until the count is less than the greatest count
                    for mat_tup, counts in mat_counts:
                        if counts < greatest:
                            break
                        pos_mats += [mat_tup]

                    mat = random.choice(pos_mats) # Chose a random mature miRNA to assign the read to
                matID, start = mat
                read_counter[matID] += 1
                if al_strand == "+":
                    for i in range(al_start-start, al_end-start+1):
                        qc_counter[i] += 1
                else:
                    for i in range(start-al_end, start-al_start+1):
                        qc_counter[i] += 1
        
        return read_counter, qc_counter


def count_sgrna(fnameList, sampleNameList, sgRNANameList):
    """
        Counts the number of reads which align to each sgRNA for each sample
        Takes a list of .sam file names/locations, a list of sample name and a list of sgRNA names
        Returns a read count dataframe with sample name columns and sgRNA rows 
    """
    # Creates a pandas 2D dataframe with sgRNA names as row names and samples as columns
    outPutDataFrame = pd.DataFrame(0.0, index=sgRNANameList, columns=sampleNameList)
    
    # Creates a second dataframe to hold summary statistics for each sample
    summaryDataFrame = pd.DataFrame(0.0, index=sampleNameList, columns=['Total Reads', 'Aligned Reads', 'Percent Aligned Reads'])
    
    # Loops through the files with the aligned reads for each sample 
    for i in range(len(fnameList)):
        fname = fnameList[i]
        sampname = sampleNameList[i]
        
        # Keeps track of the number of unaligned reads
        unaligned = 0
        
        # This makes sure the file closes properly even if there is an error while the program is running
        with open(fname, 'r') as f:
            # Counter for each sample
            sgCount = Counter()

            for line in f:
                # Ignore header lines
                if line[0] == '@':
                    continue
                
                # Finds the sgRNA the read aligned to (as sgRNAs were treated like chromosomes when creating index)
                infoList = line.split('\t') # split into a list based on tabs 
                sg = infoList[2] # the chromosome is the 3rd element in a .sam row
                
                # If the read does not align to an sgRNA, add to count of unaligned reads
                if sg == '*':
                    unaligned += 1
                    continue
                else:
                    sgCount[sg] += 1
                
            # Convert counter to dataframe
            # fills in the column for the sample with read counts
            sgNames = sgCount.keys()
            for sgRNA in sgNames:
                outPutDataFrame.set_value(sgRNA, sampname, sgCount[sgRNA])
            
            # Add the total reads and the aligned reads for the sample to the summary df
            alignedSampReads = outPutDataFrame[sampname].sum()
            totalSampReads = alignedSampReads + unaligned
            perAligned = (alignedSampReads/float(totalSampReads))*100.0
            summaryDataFrame.set_value(sampname, 'Total Reads', totalSampReads)
            summaryDataFrame.set_value(sampname, 'Aligned Reads', alignedSampReads)
            summaryDataFrame.set_value(sampname, 'Percent Aligned Reads', perAligned)
    
    return outPutDataFrame, summaryDataFrame