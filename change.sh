  git filter-branch -f --commit-filter '
  if [ "$GIT_COMMITTER_NAME" = "John Anderson" ];
  then
  GIT_COMMITTER_NAME="Song Luan";
  GIT_AUTHOR_NAME="Song Luan";
  GIT_COMMITTER_EMAIL="lsupperx@gmail.com";
  GIT_AUTHOR_EMAIL="lsupperx@gmail.com";
  git commit-tree "$@";
  else
  git commit-tree "$@";
  fi' HEAD
