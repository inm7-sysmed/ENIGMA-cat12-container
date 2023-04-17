set -e -u

# Jobs are set up to not require a shared filesystem (except for the lockfile)

# project name space
PROJECT="ENIGMA_sleep"

# define SAMPLE to be processed
SAMPLE="Sleepybrains"

# define ID for git commits (take from local user configuration)
git_name="$(git config user.name)"
git_email="$(git config user.email)"
# get current work dir
CWD=$(pwd)
# define the input ria-store only to clone from
input_store="ria+file:///data/project/sleep_ENIGMA_deprivation/dataladstore/inputstore"
# define the output ria-store to push all results to
output_store="ria+file:///data/project/sleep_ENIGMA_deprivation/dataladstore"

# define the location of the stores all analysis inputs will be obtained from
raw_store="https://github.com/OpenNeuroDatasets/ds000201.git"
# does the dataset contain sessions? [ true / false ]
ses="true"

# Build CAT container here: https://github.com/inm7-sysmed/ENIGMA-cat12-container
container_store="ria+file:///data/project/cat_preprocessed/dataladstore#~cat12.8"
container="cat12.8.1_r2042.simg"

# define the temporal working directory to clone and process each subject on
temporary_store=/tmp
# define directory of MRI datalad
MRI_dir=${SAMPLE}

# define CAT12 batch to process data
CAT_BATCH="code/cat_standalone_segment_enigma.m"


# all results a tracked in a single output dataset
# create a fresh one
# job submission will tunzipake place from a checkout of this dataset, but no
# results will be pushed into it
datalad create -c yoda ${SAMPLE}_${PROJECT}
cd ${SAMPLE}_${PROJECT}

# register a container with the CAT tooldas Ganze
datalad clone -d . "${container_store}" code/pipeline
# configure a custom container call to satisfy the needs of this analysis
datalad containers-add \
  --call-fmt 'singularity exec -B {{pwd}} --cleanenv {img} {cmd}' \
  -i code/pipeline/${container} \
  cat12-8
git commit --amend -m 'Register CAT pipeline dataset'

cp code/pipeline/batches/cat_standalone_segment_enigma_subdir.m code/cat_standalone_segment_enigma.m
datalad save -m "Import script to tune the CAT outputs for storage"

# create dedicated input and output locations. Results will be pushed into the
# output sibling, and the analysis will start with a clone from the input
# sibling.
datalad create-sibling-ria -s ${PROJECT}_in "${input_store}"  --new-store-ok
datalad create-sibling-ria -s ${PROJECT}_out "${output_store}" --new-store-ok --alias ${SAMPLE}_${PROJECT}


# register the input dataset, a superdataset comprising all participants
datalad clone -d . ${raw_store} inputs/${SAMPLE}
datalad get -n inputs/${MRI_dir}

git commit --amend -m "Register ${SAMPLE} BIDS dataset as input"

# define target dirs for datasets with & without sessions
if ${ses}; then
  dstr="-f3-4"
else
  dstr="-f3"
fi


# the actual compute job specification
cat > code/participant_job << EOT
#!/bin/bash

# the job assumes that it is a good idea to run everything in PWD
# the job manager should make sure that is true

# fail whenever something is fishy, use -x to get verbose logfiles
set -e -u -x

dssource="\$1"
pushgitremote="\$2"
subid="\$3"

# get the analysis dataset, which includes the inputs as well
# importantly, we do not clone from the lcoation that we want to push the
# results too, in order to avoid too many jobs blocking access to
# the same location and creating a throughput bottleneck
datalad clone "\${dssource}" ds

# all following actions are performed in the context of the superdataset
cd ds

# in order to avoid accumulation temporary git-annex availability information
# and to avoid a syncronization bottleneck by having to consolidate the
# git-annex branch across jobs, we will only push the main tracking branch
# back to the output store (plus the actual file content). Final availability
# information can be establish via an eventual "git-annex fsck -f ${PROJECT}_out-storage".
# this remote is never fetched, it accumulates a larger number of branches
# and we want to avoid progressive slowdown. Instead we only ever push
# a unique branch per each job (subject AND process specific name)
git remote add outputstore "\$pushgitremote"

# all results of this job will be put into a dedicated branch
git checkout -b "job-\${JOBID}"

# we pull down the input subject manually in order to discover relevant
# files. We do this outside the recorded call, because on a potential
# re-run we want to be able to do fine-grained recomputing of individual
# outputs. The recorded calls will have specific paths that will enable
# recomputation outside the scope of the original Condor setup
datalad get -n "inputs/${MRI_dir}"

# the meat of the matter
# look for T1w files in the input data for the given participant
# it is critical for reproducibility that the command given to
# "containers-run" does not rely on any property of the immediate
# computational environment (env vars, services, etc)
find \\
  inputs/${MRI_dir}/ \\
  -name "\${subid}*T1w.nii.gz" \\
  -exec sh -c '
    odir=\$(echo {} | cut -d / ${dstr});
    datalad -c datalad.annex.retry=12 containers-run \\
      -m "Compute \$odir" \\
      -n cat12-8 \\
      --explicit \\
      -o \$odir \\
      -i {} \\
      sh -e -u -x -c "
        rm -rf {outputs[0]} ;
        mkdir -p {outputs[0]} \\
        && cp {inputs[0]} {outputs[0]} \\
        && /singularity -b ${CAT_BATCH} {outputs[0]}/*.nii.gz \\
        && rm -f {outputs[0]}/*.nii* \\
        && gzip {outputs[0]}/*/*.nii \\
        " \\
  ' \\;

# remove big files from results after hashing before pushing to ria
datalad drop --what filecontent --reckless kill \
  \${subid}/*/mri/iy* \${subid}/*/mri/y* \${subid}/*/mri/anon_m* \
  \${subid}/*/mri/wj* \${subid}/*/*/*.pdf \${subid}/*/surf/*sphere* \
  \${subid}/*/surf/*pial* \${subid}/*/surf/*white*

# it may be that the above command did not yield any outputs
# and no commit was made (no T1s found for the given participant)
# we nevertheless push the branch to have a records that this was
# attempted and did not fail

# file content first -- does not need a lock, no interaction with Git
datalad push --to ${PROJECT}_out-storage
# and the output branch
flock --verbose \$DSLOCKFILE git push outputstore

echo SUCCESS
# job handler should clean up workspace
EOT

chmod +x code/participant_job
datalad save -m "Participant compute job implementation"


cat > code/process.sub << EOT
#!/bin/bash

subject=\$1

executable=\$(pwd)/code/participant_job

# the job expects these environment variables for labeling and synchronization
# - JOBID: subject AND process specific ID to make a branch name from
#     (must be unique across all (even multiple) submissions)
#     including the cluster ID will enable sorting multiple computing attempts
# - DSLOCKFILE: lock (must be accessible from all compute jobs) to synchronize
#     write access to the output dataset
# - DATALAD_GET_SUBDATASET__SOURCE__CANDIDATE__...:
#     (additional) locations for datalad to locate relevant subdatasets, in case
#     a configured URL is outdated
# - GIT_AUTHOR_...: Identity information used to save dataset changes in compute
#     jobs
export JOBID=\${subject} \\
  DSLOCKFILE=\$(pwd)/.condor_datalad_lock \\
  GIT_AUTHOR_NAME='${git_name}' \\
  GIT_AUTHOR_EMAIL='${git_email}'

# essential args for "participant_job"
# 1: where to clone the analysis dataset
# 2: location to push the result git branch to. The "ria+" prefix is stripped.
# 3: ID of the subject to process
arguments="${input_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset) \\
  $(git remote get-url --push ${PROJECT}_out) \\
  \${subject} \\
  "

mkdir -p ${temporary_store}/tmp_\${subject:4}
cd ${temporary_store}/tmp_\${subject:4}

\${executable} \${arguments} \\
> $(pwd)/logs/\${subject}.out \\
2> $(pwd)/logs/\${subject}.err

chmod 777 -R ${temporary_store}/tmp_\${subject:4} && \
rm -rf ${temporary_store}/tmp_\${subject:4}

EOT

chmod +x code/process.sub
datalad save -m "individual job submission"


cat > code/results.merger << EOT
#!/bin/bash
# finalize FAIRly Big Workflow dataset

# merge results brachnes into master
datalad update
git merge -m "Merge results" \$(git branch -al | grep 'job-' | tr -d ' ')
# clean git annex branch
git annex fsck -f ${PROJECT}_out-storage --fast
# declare local data clone as dead
git annex dead here
# datalad push merged results
datalad push --data nothing --to ${PROJECT}_out

EOT
chmod +x code/results.merger
datalad save -m "finalize dataset by merging results branches into master"


# the logfiles folder is to be ignored by git
mkdir logs
echo logs >> .gitignore

###############################################################################
# HTCONDOR SETUP START - FIXME remove or adjust this according to your needs.
###############################################################################

# HTCondor compute setup
# the workspace is to be ignored by git
echo dag_tmp >> .gitignore
echo .condor_datalad_lock >> .gitignore

## define ID for git commits (take from local user configuration)
git_name="$(git config user.name)"
git_email="$(git config user.email)"

# compute environment for a single job
#-------------------------------------------------------------------------------
# FIXME: Adjust job requirements to your needs

cat > code/process.condor_submit << EOT
universe       = vanilla
# resource requirements for each job
request_cpus   = 1
request_memory = 3G
request_disk   = 4G

# be nice and only use free resources
nice_user = true

# tell condor that a job is self contained and the executable
# is enough to bootstrap the computation on the execute node
should_transfer_files = yes
# explicitly do not transfer anything back
# we are using datalad for everything that matters
transfer_output_files = ""

# the actual job script, nothing condor-specific in it
executable     = \$ENV(PWD)/code/participant_job

# the job expects these environment variables for labeling and synchronization
# - JOBID: subject AND process specific ID to make a branch name from
#     (must be unique across all (even multiple) submissions)
#     including the cluster ID will enable sorting multiple computing attempts
# - DSLOCKFILE: lock (must be accessible from all compute jobs) to synchronize
#     write access to the output dataset
# - DATALAD_GET_SUBDATASET__SOURCE__CANDIDATE__...:
#     (additional) locations for datalad to locate relevant subdatasets, in case
#     a configured URL is outdated
# - GIT_AUTHOR_...: Identity information used to save dataset changes in compute
#     jobs
environment = "\\
  JOBID=\$(subject).\$(Cluster) \\
  DSLOCKFILE=\$ENV(PWD)/.condor_datalad_lock \\
  GIT_AUTHOR_NAME='${git_name}' \\
  GIT_AUTHOR_EMAIL='${git_email}' \\
  "

# place the job logs into PWD/logs, using the same name as for the result branches
# (JOBID)
log    = \$ENV(PWD)/logs/\$(subject)_\$(Cluster).log
output = \$ENV(PWD)/logs/\$(subject)_\$(Cluster).out
error  = \$ENV(PWD)/logs/\$(subject)_\$(Cluster).err
# essential args for "participant_job"
# 1: where to clone the analysis dataset
# 2: location to push the result git branch to. The "ria+" prefix is stripped.
# 3: ID of the subject to process
arguments = "\\
  ${input_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset) \\
  $(git remote get-url --push ${PROJECT}_out) \\
  \$(subject) \\
  "
queue
EOT

# ------------------------------------------------------------------------------
# FIXME: Adjust the find command below to return the unit over which your
# analysis should parallelize. Here, subject directories on the first hierarchy
# level in the input data are returned by searching for the "sub-*" prefix.
# The setup below creates an HTCondor DAG.
# ------------------------------------------------------------------------------
# processing graph specification for computing all jobs
cat > code/process.condor_dag << "EOT"
# Processing DAG
EOT
for s in $(find inputs/${MRI_dir} -maxdepth 1 -name 'sub-*' -printf '%f\n'); do
  printf "JOB ${s%.*} code/process.condor_submit\nVARS ${s%.*} subject=\"$s\"\n" >> code/process.condor_dag
done
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

datalad save -m "HTCondor submission setup" code/ .gitignore

################################################################################
# HTCONDOR SETUP END
################################################################################

# ria store for input data
datalad push --to ${PROJECT}_in
# now cleanup input
datalad uninstall -r --nocheck inputs/${SAMPLE}

# make sure the fully configured output dataset is available from the designated
# ria store for output data
datalad push --to ${PROJECT}_out

# submit condor dag to process as backfill jobs
# condor_submit_dag code/process.condor_dag

# if we get here, we are happy
echo SUCCESS
