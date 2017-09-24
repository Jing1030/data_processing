import matplotlib.pyplot as plt
import pandas as pd

from matplotlib_venn import venn2, venn3

def make_diagram(group_list, name_list, title, output_file, return_names=False,
                 figsize=(8, 6), high_qual=False):
    """
        Makes venn diagram

        group_list: list of lists of values in the given group
        name_list: list of names for each group
        title: title for the venn diagram
        output_file: name of file to save diagram to
        return_names [optional]: returns the names/values in overlap of all groups
        figsize [optional]: size (in inches) of figure
        high_qual [optional]: returns a higher quality (800 dpi) figure
    """
    fig, ax = plt.subplots(1,1, figsize=figsize)

    # if we are only comparing two groups
    if len(group_list) == 2: 
        group_one_only = [x for x in group_list[0] if x not in group_list[1]]
        group_two_only = [x for x in group_list[1] if x not in group_list[0]]
        both = [x for x in group_list[0] if x in group_list[1]]

        venn2(ax=ax, subsets=(len(group_one_only), len(group_two_only), len(both)),
              set_labels=(name_list[0], name_list[1]))

    elif len(group_list) == 3:
        group_one_only = [x for x in group_list[0] if x not in group_list[1] and x not in group_list[2]]
        group_two_only = [x for x in group_list[1] if x not in group_list[0] and x not in group_list[2]]
        group_one_two = [x for x in group_list[0] if x in group_list[1] and x not in group_list[2]]
        group_three_only = [x for x in group_list[2] if x not in group_list[1] and x not in group_list[0]]
        group_one_three = [x for x in group_list[0] if x not in group_list[1] and x in group_list[2]]
        group_two_three = [x for x in group_list[1] if x not in group_list[0] and x in group_list[2]]
        all_groups = [x for x in group_list[0] if x in group_list[1] and x in group_list[2]]
        
        venn3(ax=ax, subsets=(len(group_one_only), len(group_two_only), len(group_one_two), len(group_three_only),
                              len(group_one_three), len(group_two_three), len(all_groups)), 
              set_labels=(name_list[0], name_list[1], name_list[2]))
    else:
        print "Error: please enter a condition list with 2 or 3 conditions"
        return

    ax.set_title(title)

    if high_qual:
        plt.savefig(output_file, bbox_inches="tight", dpi=800)
    else:
        plt.savefig(output_file, bbox_inches="tight")
    plt.close()

    if return_names:
        if len(group_list) == 2:
            return both
        else:
            return all_groups
    else:
        return
