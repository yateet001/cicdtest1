package com.visa.Embed.Controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SampleSecurityController {

    @GetMapping("/")
    public String index() {
        return "index";
    }

    /**
     * Token details endpoint
     * Demonstrates how to extract and make use of token details
     * For full details, see method: Utilities.filterclaims(OidcUser principal)
     * 
     * @param model     Model used for placing claims param and bodyContent param in
     *                  request before serving UI.
     * @param principal OidcUser this object contains all ID token claims about the
     *                  user. See utilities file.
     * @return String the UI.
     */
    @GetMapping(path = "/token_details")
    public String tokenDetails() {
        return "token";
    }

}