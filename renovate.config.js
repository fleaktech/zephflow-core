module.exports = {
  platform: 'github',
  endpoint: 'https://api.github.com/',

  // Process this repository
  repositories: ['fleaktech/zephflow-core'],

  // Use renovate.json in repo for package rules, schedules, and hostRules
  // This allows renovate.json to control scheduling, grouping, etc.
  onboarding: false,
  requireConfig: 'optional',

  // Git author for commits
  gitAuthor: 'Renovate Bot <bot@renovateapp.com>',

  // Enable Gradle version catalog support
  gradle: {
    managerFilePatterns: [
      '/(^|/)gradle\\.properties$/',
      '/\\.gradle(\\.kts)?$/',
      '/(^|/)gradle/libs\\.versions\\.toml$/',
    ],
  },

  // PR settings
  branchPrefix: 'renovate/',
};
