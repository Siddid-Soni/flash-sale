// njs: picks the rate-limit bucket for a request from its bearer token.
//
// The token is NOT verified here. The result only chooses which limit_req
// bucket the request is counted in; the Order Service verifies the signature
// and rejects a forged token with 401 before anything happens. Forging a `sub`
// can therefore change which bucket a request lands in, but never grants
// access; checkout also has a per-IP limit, so rotating forged subs from one
// address does not escape throttling.

// The user id (digits only, as the Order Service issues it), or '' when the
// request has no usable bearer token.
function sub(r) {
    var auth = r.headersIn['Authorization'];
    if (!auth || auth.slice(0, 7) !== 'Bearer ') {
        return '';
    }
    var parts = auth.slice(7).trim().split('.');
    if (parts.length !== 3) {
        return '';
    }
    try {
        var claims = JSON.parse(Buffer.from(parts[1], 'base64url').toString());
        var id = claims && claims.sub;
        return typeof id === 'string' && /^[0-9]{1,10}$/.test(id) ? id : '';
    } catch (e) {
        return '';
    }
}

export default { sub };
