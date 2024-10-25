#%%

import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


# %%
# dbfile = "/home/tpan/src/chorus-extract-upload/journal.20240925_waveforms.db"
dbfile = "/home/tpan/src/chorus-extract-upload/journal_images_partial_2.db"



# %%
def plot_violin(df, x_column, y_column, bins):
    # stratify the data into bins.
    nandf = df[df[y_column].notna() & df[x_column].notna()]
    
    print("number of rows: ", len(nandf))
    print("number of rows with NaN: ", len(df) - len(nandf))
    
    fig, axes = plt.subplots()
    my_plot = sns.boxplot(data=nandf, x =x_column, y=  y_column, ax = axes)
    
    # axes.violinplot(dataset = partitioned)
    axes.set_title(f'{y_column} vs {x_column}')
    axes.yaxis.grid(True)
    axes.set_xlabel(x_column)
    axes.set_ylabel(y_column)
    my_plot.set_xticklabels(bins[1:], rotation=90)
    # axes.set_xticks(bin_edges)
    plt.show()


# %%
def analyze(dbfile:str, modality:str, upload_start: str = None, upload_end: str = None):
    # open the sqlite database
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()

    # get the size, md5_duration, upload_duration, and verify_duration from the journal table, for rows where the upload_dtstr is later than 20240920000000
    result = c.execute(f"SELECT size, upload_dtstr, md5_duration, upload_duration, verify_duration FROM journal where MODALITY = '{modality}'").fetchall()

    # close the connection
    conn.close()


    # create a pandas dataframe from the results
    orig_df = pd.DataFrame(result, columns=["size", "upload_dtstr", "md5_duration", "upload_duration", "verify_duration"])

    orig_df['size'] = orig_df['size'].astype('int') / (1024*1024)
    orig_df['upload_throughput'] = orig_df['size'] / orig_df['upload_duration']

    if upload_start is not None:
        df = orig_df[(orig_df['upload_dtstr'] > upload_start)]
        
        if upload_end is not None:
            df = df[df['upload_dtstr'] < upload_end]
        # df = orig_df[orig_df['upload_dtstr'] > '20240921000000']
    else:
        if upload_end is not None:
            df = orig_df[orig_df['upload_dtstr'] < upload_end]
        else:
            df = orig_df

    print("total files: ", len(df))
    print("total size: ", np.nansum(df['size']))
    print("total md5 time: ", np.nansum(df['md5_duration']))
    print("total upload time: ", np.nansum(df['upload_duration']))
    print("total verify time: ", np.nansum(df['verify_duration']))
    print("min/max throughtput: ", np.nanmin(df['upload_throughput']), np.nanmax(df['upload_throughput']))


    # create a size histogram
    df.hist(column='size', bins=20)
    plt.yscale('log')
    if modality == 'Waveforms':
        hist, bin_edges = np.histogram(np.log2(df['size']), bins=20) # log scale to partition the bins
        bin_edges = np.round(np.exp2(bin_edges), 6)  # convert edges back to linear scale
    else:
        hist, bin_edges = np.histogram(df['size'], bins=20) # log scale to partition the bins
        bin_edges = np.round(bin_edges, 6)  # convert edges back to linear scale
    print(bin_edges)
    # place histogram of sizes as a column
    df['size (MiB)'] = pd.cut(df['size'], bins=bin_edges, labels=bin_edges[1:])
    return df, bin_edges


# %%
df, bin_edges = analyze(dbfile, 'Waveforms')

#%%
# plot the md5_duration as a min-max boxplot, binned by size
plot_violin(df, 'size (MiB)', 'md5_duration', bin_edges)

# plot the upload_duration as a min-max boxplot, binned by size
plot_violin(df, 'size (MiB)', 'upload_duration', bin_edges)

# plot the verify_duration as a min-max boxplot, binned by size
plot_violin(df, 'size (MiB)', 'verify_duration', bin_edges)

# plot the upload_throughput as a min-max boxplot, binned by size
plot_violin(df, 'size (MiB)', 'upload_throughput', bin_edges)



#%%
# new waveforms
df2, bin_edges = analyze(dbfile, 'Waveforms', upload_start = '20240921000000')

#%%
# plot the md5_duration as a min-max boxplot, binned by size
plot_violin(df2, 'size (MiB)', 'md5_duration', bin_edges)

# plot the upload_duration as a min-max boxplot, binned by size
plot_violin(df2, 'size (MiB)', 'upload_duration', bin_edges)

# plot the verify_duration as a min-max boxplot, binned by size
plot_violin(df2, 'size (MiB)', 'verify_duration', bin_edges)

# plot the upload_throughput as a min-max boxplot, binned by size
plot_violin(df2, 'size (MiB)', 'upload_throughput', bin_edges)


#%%
# images
df3, bin_edges = analyze(dbfile, 'Images')

#%%
# plot the md5_duration as a min-max boxplot, binned by size
plot_violin(df3, 'size (MiB)', 'md5_duration', bin_edges)

# plot the upload_duration as a min-max boxplot, binned by size
plot_violin(df3, 'size (MiB)', 'upload_duration', bin_edges)

# plot the verify_duration as a min-max boxplot, binned by size
plot_violin(df3, 'size (MiB)', 'verify_duration', bin_edges)

# plot the upload_throughput as a min-max boxplot, binned by size
plot_violin(df3, 'size (MiB)', 'upload_throughput', bin_edges)


# %%
