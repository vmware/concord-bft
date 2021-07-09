module.exports = {
    extends: ['@commitlint/config-conventional'],
    rules: {
        'footer-leading-blank': [2, 'always'],
        'footer-max-line-length': [2, 'always', 120],
        'body-max-line-length': [2, 'always', 120],
    },
};
