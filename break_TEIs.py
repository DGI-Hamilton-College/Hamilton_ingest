'''
Created on Sep 19, 2011

@author: William Panting
'''

from islandoraUtils import fileManipulator
import os, sys
from os import path

if __name__ == '__main__':
    if len(sys.argv) == 2:
        source_directory = sys.argv[1]
        destination_directory=os.path.join(source_directory,'islandora')
    elif len(sys.argv) == 3:
        source_directory = sys.argv[1]
        destination_directory = sys.argv[2]
    else:
        print('Please verify source and/or destination directory.')
        sys.exit(-1)
        
    #if source directory is not
    if not os.path.isdir(source_directory):
        print('Indicated source directory is not a directory.')
        sys.exit(-1)
        
    #if the destination directory doesn't exist create it
    if not os.path.isdir(destination_directory):
        os.mkdir(destination_directory)
        
    #run through each file in the directory
    for file in os.listdir(source_directory):
        file_path = os.path.join(source_directory, file)
        if os.path.isfile(file_path) and (file_path.endswith('.xml') or file_path.endswith('.tei') or file_path.endswith('.TEI') or file_path.endswith('.XML')):
            fileManipulator.breakTEIOnPages(file_path, destination_directory, force_numeric_page_numbers = True)